from pyspark.sql import functions as F

min_shipping_fee = df_orders.agg(F.min("shipping_fee")).first()[0]

result = (
    df_orders
    .filter(F.col("shipping_fee") == min_shipping_fee)
    .select("id", "shipping_fee", "store_id")
)
