from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(F.col("s.cost"), F.col("s.carrier"), F.col("o.status"))
    .orderBy("status", F.col("carrier").desc())
)
