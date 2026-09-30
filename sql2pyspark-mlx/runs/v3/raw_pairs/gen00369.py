from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(202, 349))
    .select("launch_date")
    .intersect(df_orders.select("order_date"))
    .orderBy("launch_date")
)
