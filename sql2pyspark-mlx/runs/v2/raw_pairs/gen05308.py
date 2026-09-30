from pyspark.sql import functions as F

max_shipping_fee = df_orders.agg(F.max("shipping_fee")).first()[0]

result = (
    df_orders
    .filter(F.col("shipping_fee") >= max_shipping_fee)
    .select("id", "shipping_fee")
    .orderBy(F.col("shipping_fee").desc(), "id")
    .limit(1)
)
