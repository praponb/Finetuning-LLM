from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .intersect(df_products.select("cost"))
    .orderBy("shipping_fee")
)
