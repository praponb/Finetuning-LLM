from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.pct_lost") >= 64)
    .select(F.col("o.store_id"), F.col("o.amount"), F.col("os.pct_lost"))
)
