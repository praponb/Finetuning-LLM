from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("salary") > 74000)
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
)

summary = (
    valid_employees
    .groupBy("is_remote", "job_title")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    summary
    .filter(F.col("max_salary") > 132000)
    .select("is_remote", "job_title", "max_salary")
    .orderBy(F.col("job_title").desc())
)
