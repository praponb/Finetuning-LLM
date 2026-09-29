from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 720) & F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "price",
        F.array_join("tags", ", ").alias("tags_text"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)
