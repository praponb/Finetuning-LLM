from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.delivered_date") > "2023-04-21")
    .select(F.col("s.status"), F.col("o.status").alias("order_status"))
)
