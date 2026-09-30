from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.store_id").isNull())
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.status"))
    .orderBy("id")
    .limit(1)
)
