from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select("id", "paid_at", F.substring("method", 1, 3).alias("method_prefix"))
    .orderBy(F.col("id").desc(), F.col("paid_at").desc())
)
