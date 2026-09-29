from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.cnt") > 5)
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.shipping_fee"), F.col("ot.cnt"))
)
