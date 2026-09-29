from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .subtract(df_orders.select("order_date"))
)
