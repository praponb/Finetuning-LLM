from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & (F.col("country") != "US"))
    .select("id", "name", "credit_limit")
    .orderBy(F.col("id").desc())
)
