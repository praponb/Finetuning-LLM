from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .intersect(df_products.select("launch_date"))
)
