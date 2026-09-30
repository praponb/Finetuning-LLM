from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Initech") & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        F.regexp_replace("product_name", " ", "-").alias("product_name_slug"),
        F.array_join("tags", ", ").alias("tags_text"),
    )
)
