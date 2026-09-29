from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 440)
    .select(
        "id",
        "tags",
        "brand",
        F.round((F.col("price") * 0.85), 2).alias("price_adjusted"),
    )
    .orderBy(F.col("price_adjusted").desc(), F.col("brand").desc())
)
