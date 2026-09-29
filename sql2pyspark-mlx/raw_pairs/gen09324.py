from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "is_remote", "full_name")
    .orderBy(F.col("id").desc(), "full_name")
)
