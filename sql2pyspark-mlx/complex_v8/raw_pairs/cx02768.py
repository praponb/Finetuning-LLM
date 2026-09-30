from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

result = (
    filtered_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "unit_price",
        "unit_price_bucket",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
    .orderBy("unit_price_bucket", F.col("unit_price_per_qty").asc())
)
