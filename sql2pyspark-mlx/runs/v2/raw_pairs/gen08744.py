from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.channel") != "mobile")
    .select(F.col("s.id"), F.col("s.cost"), F.col("o.channel"), F.col("o.shipping_fee"))
    .orderBy(F.col("id").desc(), "cost")
)
