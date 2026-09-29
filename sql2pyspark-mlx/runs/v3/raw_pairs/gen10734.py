from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.filter(F.col("launch_date") < "2023-09-01").alias("p"), F.col("ss.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("ss.id"), F.col("ss.revenue"), F.col("ss.units"))
)
