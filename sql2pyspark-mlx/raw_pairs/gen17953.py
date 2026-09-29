from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.month("shipped_date").isin([5, 9, 12])).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .filter(F.col("o.store_id").isNull())
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.channel"))
    .orderBy("channel")
)
