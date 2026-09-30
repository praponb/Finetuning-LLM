from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("age") >= 62) & (F.col("credit_limit") <= 8200))
    .select("country", "segment", "is_active")
    .orderBy("segment")
)
