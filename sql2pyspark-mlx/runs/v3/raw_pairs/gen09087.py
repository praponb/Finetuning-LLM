from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Lapto%"))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        F.substring("product_name", 1, 2).alias("product_name_prefix"),
    )
    .orderBy(F.col("category").desc(), "id")
    .limit(50)
)
