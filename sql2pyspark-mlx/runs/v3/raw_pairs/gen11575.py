from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("shipping_fee") <= 22).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.cost") <= 79)
    .select(F.col("s.id"), F.col("s.status"), F.col("s.carrier"))
)
