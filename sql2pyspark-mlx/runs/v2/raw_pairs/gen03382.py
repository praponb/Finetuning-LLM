from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") < 700) & (F.array_contains("tags", "new") | (F.col("price") > 660)))
    .select(
        "id",
        "price",
        F.concat_ws("-", "brand", "category").alias("brand_category_key"),
    )
)
