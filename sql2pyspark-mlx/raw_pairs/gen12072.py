from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(~F.col("status").isin(["DELIVERED", "RETURNED"]))
    .groupBy("order_id")
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.kerry_count") >= 3)
    .select(
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("os.min_shipped_date"),
        F.col("os.kerry_count"),
    )
    .orderBy(F.col("min_shipped_date").desc())
)
