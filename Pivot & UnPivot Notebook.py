# Databricks notebook source
data = [(1,'sridhar','male','HR'),\
        (2,'shruthi','female','IT'),\
        (3,'sandeep','male','IT'),\
        (4,'deepthi','female','Payroll'),\
        (5,'srilatha','female','HR'),\
        (6,'pradeep','male','Payroll'),\
        (7,'pavani','female','IT'),\
        (8,'anji','male','HR'),\
        (9,'mouni','female','IT')]

schema = ["ID","NAME","GENDER","DEPT"]

df = spark.createDataFrame(data,schema)

df.show(truncate=False)

# COMMAND ----------

df.groupBy("DEPT","GENDER").count().show()

# COMMAND ----------

pivotDF = df.groupBy("DEPT").pivot("GENDER").count()
pivotDF.show()
unpivotDF = pivotDF.select('DEPT',expr("stack(2,'female',female,'male',male) as (GENDER, COUNT)"))
unpivotDF.show()

# COMMAND ----------

df.createOrReplaceGlobalTempView("glob_department")
df.createOrReplaceTempView("department")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from department

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from global_temp.glob_department

# COMMAND ----------


