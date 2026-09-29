from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") >= 470) & (F.col("stock_qty") <= 379))
    .select(
        "id",
        "price",
        "product_name",
        "tags",
        F.upper("brand").alias("brand_upper"),
    )
    .orderBy(F.col("product_name").desc())
)
