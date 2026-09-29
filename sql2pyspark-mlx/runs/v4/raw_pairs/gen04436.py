from pyspark.sql import functions as F

order_stats = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-10-01", "2025-04-01"))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .filter(F.col("O.kerry_count") >= 43)
    .select(F.col("o.order_date"), F.col("o.channel"), F.col("O.kerry_count"))
    .orderBy(F.col("channel").desc(), "order_date")
)
