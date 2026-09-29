from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .union(df_store_sales.select("revenue"))
    .orderBy("shipping_fee")
)
