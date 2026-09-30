from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") <= 260) & F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "price",
        "launch_date",
        F.lower("product_name").alias("product_name_lower"),
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
    )
)
