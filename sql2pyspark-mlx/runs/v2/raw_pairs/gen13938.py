from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "clearance") & ((F.size("tags") >= 1) | (F.col("price") > 600)))
    .select(
        "id",
        "category",
        F.round((F.col("cost") * 0.85), 2).alias("cost_adjusted"),
        F.concat_ws("/", "product_name", "category").alias("product_name_category_key"),
    )
)
