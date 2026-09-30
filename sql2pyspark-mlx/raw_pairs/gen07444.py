from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .intersect(df_orders.filter(F.col("amount") > 900).select("shipping_fee"))
)
