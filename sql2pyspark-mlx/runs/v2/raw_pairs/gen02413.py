from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount") >= 770).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.delivered_date") >= "2023-09-01")
    .select(F.col("s.id"), F.col("s.delivered_date"), F.col("s.carrier"))
)
