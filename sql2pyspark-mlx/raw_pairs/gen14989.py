from pyspark.sql import functions as F

base = df_orders.filter((F.col("discount") < 0.36) | (F.year("order_date") >= 2025))

orders_stats = (
    base
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", F.col("discount")).otherwise(0)).alias("partner_discount"),
    )
)

result = (
    orders_stats
    .filter(F.col("partner_discount") < 57)
    .select("store_id", "partner_discount")
    .orderBy("store_id")
)
