from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("id", "dept_id", "is_remote", F.upper("full_name").alias("full_name_upper"))
)
