from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("qty") > 137)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.abs(F.col("unit_price") - 258.6).alias("unit_price_distance"),
    )
)

result = (
    prepared_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_distance",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
        F.round(F.col("unit_price_distance") * 1.5, 2).alias("unit_price_distance_adj"),
    )
    .orderBy(F.col("sku_id").desc(), F.col("move_type_prefix").asc())
)
