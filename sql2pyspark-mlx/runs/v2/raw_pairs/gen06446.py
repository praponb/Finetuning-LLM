from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("channel") == "store").alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.status"), F.col("s.delivered_date"))
    .orderBy(F.col("delivered_date").desc())
)
