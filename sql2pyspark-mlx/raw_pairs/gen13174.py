from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 920)
    .select("cost")
    .union(df_orders.select("shipping_fee"))
)
