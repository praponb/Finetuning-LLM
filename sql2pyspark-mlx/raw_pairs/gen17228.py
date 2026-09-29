from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").isNotNull())
    .select("launch_date")
    .union(df_orders.filter(F.col("order_date").isNotNull()).select("order_date"))
    .distinct()
)
