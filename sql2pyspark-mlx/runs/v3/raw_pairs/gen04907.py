from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2024-04-11")
    .select("country", "segment")
    .distinct()
    .orderBy(F.col("country").desc())
)
