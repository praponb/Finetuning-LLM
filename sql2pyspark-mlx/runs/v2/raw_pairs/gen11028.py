from pyspark.sql import functions as F

result = (
    df_employees
    .select("id", "full_name")
    .orderBy("full_name", F.col("id").desc())
)
