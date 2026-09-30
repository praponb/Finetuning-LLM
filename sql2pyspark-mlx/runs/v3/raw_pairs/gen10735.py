from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(147, 351))
    .select("launch_date")
    .union(df_orders.select("order_date"))
)
