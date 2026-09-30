from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "category", "stock_qty", F.length("brand").alias("brand_length"))
)
