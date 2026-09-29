from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("ot.min_shipped_date"))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(5)
)
