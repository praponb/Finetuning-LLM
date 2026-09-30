from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2025-03-01")
    .select("stock_qty", F.length("brand").alias("brand_length"))
    .orderBy("stock_qty")
)
