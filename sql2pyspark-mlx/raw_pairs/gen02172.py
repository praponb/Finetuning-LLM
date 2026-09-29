from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.lower("email").like("%vy@%"))
    .select("id", "segment")
)
