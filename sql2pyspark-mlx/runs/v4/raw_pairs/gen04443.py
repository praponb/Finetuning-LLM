from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .intersect(df_orders.filter(F.col("shipping_fee") < 34).select("shipping_fee"))
    .orderBy(F.col("price").desc())
)
