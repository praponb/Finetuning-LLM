from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", "is_remote", "full_name")
    .orderBy(F.col("full_name").desc())
)
