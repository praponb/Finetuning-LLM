from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("carrier") == "Flash").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.channel") == "partner")
    .select(F.col("o.id"), F.col("o.channel"))
)
