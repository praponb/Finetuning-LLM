from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.lower(F.trim("product_name")) == "office chair") & (F.col("brand").isNull() | (F.col("cost") < 330)))
    .select("category", "product_name")
)
