from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-10-28 18:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select("*", F.substring("status", 1, 2).alias("status_prefix"))
)

result = (
    base
    .groupBy("status")
    .pivot("method", ["cash", "card"])
    .agg(F.max("amount"))
)
