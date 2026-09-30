from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "tags",
        "product_name",
        "category",
        (F.col("cost") / F.nullif("stock_qty", F.lit(0))).alias("cost_per_stock_qty"),
        F.array_contains("tags", "bestseller").alias("is_bestseller"),
    )
)
