from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("shipping_fee")
    .union(df_products.filter(F.col("price").between(630, 800)).select("cost"))
)
