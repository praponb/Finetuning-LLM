from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .select(
        "id",
        "stock_qty",
        "category",
        "price",
        F.trim("category").alias("clean_category"),
    )
)
