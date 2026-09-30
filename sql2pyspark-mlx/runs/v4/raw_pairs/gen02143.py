from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "full_name")
    .orderBy(F.col("full_name").desc())
)
