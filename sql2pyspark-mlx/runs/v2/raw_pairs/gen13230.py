from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.size("tags") >= 1) & F.col("product_name").like("%Hub%"))
    .select("id", "launch_date", "brand")
)
