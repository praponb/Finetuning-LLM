from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("store_id").isNull()).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.status") == "RETURNED")
    .select(F.col("s.id"), F.col("s.delivered_date"), F.col("s.shipped_date"))
)
