from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 25)
    .select("order_date")
    .union(df_products.select("launch_date"))
    .distinct()
    .orderBy("order_date")
)
