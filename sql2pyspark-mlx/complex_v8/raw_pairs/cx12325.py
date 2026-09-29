from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
)

result = (
    valid
    .select(
        "id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "move_type_prefix",
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
    )
    .orderBy("unit_price", "id")
    .limit(5)
)
