from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.upper(F.trim("product_name")) == "DESK LAMP")
    .select("id", "category")
    .orderBy(F.col("category").desc())
)
