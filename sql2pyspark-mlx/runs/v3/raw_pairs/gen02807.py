from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("channel") == "store").alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.order_id").isNotNull())
    .select(F.col("s.id"), F.col("s.order_id"))
    .orderBy("order_id")
)
