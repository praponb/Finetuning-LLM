from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("store_id").isin([22, 31, 32]) & (F.col("store_id").isNotNull() | (F.col("shipping_fee") < 15)))
)

summary = (
    recent_orders
    .groupBy("channel")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    summary
    .filter(F.col("total_amount") >= 2800)
    .select("channel", "total_amount")
    .orderBy("channel")
    .limit(1)
)
