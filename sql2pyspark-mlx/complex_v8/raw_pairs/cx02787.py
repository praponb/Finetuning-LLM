from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_eve")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 145).alias("qty_distance"),
    )
)

with_details = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("sk.category") == "Food")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.qty_distance",
        F.col("sk.category").alias("sku_category"),
    )
)

result = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "unit_price",
        "qty_distance",
        "sku_category",
        F.concat(F.col("operator"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("operator_ref"),
    )
)
