from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date") < "2025-10-01").alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.delivered_date"), F.col("s.order_id"))
    .orderBy("delivered_date", "id")
    .limit(5)
)
