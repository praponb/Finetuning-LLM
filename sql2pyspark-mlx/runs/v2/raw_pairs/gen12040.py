from pyspark.sql import functions as F

recent_employees = (
    df_employees
    .filter((F.col("level") != "L1") & F.col("job_title").isNull() & F.col("full_name").like("%Noa%"))
)

job_title_summary = (
    recent_employees
    .groupBy("job_title")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = job_title_summary.filter(F.col("unique_managers") > 10)
