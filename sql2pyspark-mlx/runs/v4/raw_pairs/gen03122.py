from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "store"]))
    .select("shipping_fee")
    .union(df_products.select("price"))
)
