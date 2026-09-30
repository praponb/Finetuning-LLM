from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull() & F.col("full_name").like("%Ross%") & (F.col("salary") > 102000))
    .select("job_title", "is_remote")
)
