from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 970)
    .select("launch_date")
    .subtract(df_orders.select("order_date"))
)
