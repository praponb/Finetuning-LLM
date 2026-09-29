from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("status") == "REFUNDED") & F.col("order_id").isin([5, 9, 16, 30]))
    .select("id", "status", "order_id")
    .orderBy(F.col("id").desc())
)
