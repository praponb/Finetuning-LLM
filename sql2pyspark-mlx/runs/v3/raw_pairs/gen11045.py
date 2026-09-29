from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .unionByName(df_products.select(F.col("launch_date").alias("order_date")))
    .orderBy("order_date")
)
