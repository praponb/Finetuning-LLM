from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount") > 1260).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.carrier"))
)
