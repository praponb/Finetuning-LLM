from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("cost") >= 27)
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
        F.sum("cost").alias("total_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("os.pct_lost"),
        F.col("os.total_cost"),
    )
    .orderBy(F.col("status").desc(), F.col("id").desc())
)
