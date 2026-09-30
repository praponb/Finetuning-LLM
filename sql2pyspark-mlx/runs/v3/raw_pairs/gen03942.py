from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        F.size("tags").alias("tags_count"),
        F.trim("brand").alias("clean_brand"),
    )
)
