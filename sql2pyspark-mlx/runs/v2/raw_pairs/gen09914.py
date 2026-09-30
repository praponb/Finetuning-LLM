from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .withColumn("full_name_title", F.initcap(F.trim("full_name")))
)

employees_stats = (
    base
    .groupBy("job_title", "is_remote")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    employees_stats
    .select("job_title", "is_remote", "min_salary")
    .orderBy("job_title")
)
