from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("level_lower", F.lower("level"))
    .orderBy(F.col("id").desc(), "job_title")
    .limit(3)
)
