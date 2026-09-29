from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "category",
        F.array_contains("tags", "bestseller").alias("is_bestseller"),
    )
)
