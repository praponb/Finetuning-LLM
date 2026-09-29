from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("store_id").isin([13, 23, 26]) & F.month("order_date").isin([1, 7, 11]))
)

summary = (
    recent_orders
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    summary
    .filter(F.col("partner_count") < 39)
    .select("status", "partner_count")
    .orderBy("status")
)
