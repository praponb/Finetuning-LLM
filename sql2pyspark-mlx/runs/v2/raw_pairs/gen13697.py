from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "brand",
        "cost",
        "product_name",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
    )
)
