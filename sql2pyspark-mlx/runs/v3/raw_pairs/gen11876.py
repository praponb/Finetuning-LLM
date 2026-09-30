from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount") > 440).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.cost") >= 87)
    .select(F.col("s.id"), F.col("s.delivered_date"), F.col("s.order_id"))
)
