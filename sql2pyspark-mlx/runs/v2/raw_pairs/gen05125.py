from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & F.col("is_remote"))
    .select("id", "salary", F.initcap(F.trim("job_title")).alias("job_title_title"))
    .orderBy("job_title_title", F.col("id").desc())
)
