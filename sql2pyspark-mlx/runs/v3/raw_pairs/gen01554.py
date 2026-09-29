from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .union(df_orders.select("shipping_fee"))
)
