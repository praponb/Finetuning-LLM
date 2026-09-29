from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.filter(F.col("sale_date") < "2023-08-01").alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left_anti")
    .select(F.col("p.id"), F.col("p.launch_date"))
)
