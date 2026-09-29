from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Grocery", "Books"]))
    .select("category", "product_name", F.lower("brand").alias("brand_lower"))
)
