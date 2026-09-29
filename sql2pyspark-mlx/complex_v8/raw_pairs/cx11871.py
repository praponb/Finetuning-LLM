from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("unit_price").between(111.7, 307.8))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        (F.col("qty") - F.col("unit_price")).alias("qty_minus_unit_price"),
    )
)

joined_sku = (
    base_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.days_since_moved_at",
        "s.qty_minus_unit_price",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

screened = (
    joined_sku.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.year(F.col("w.opened_on")) == 2024), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "days_since_moved_at",
        "qty_minus_unit_price",
        "sku_category",
        "sku_weight_kg",
        F.concat(F.col("sku_category"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("sku_category_ref"),
    )
    .orderBy(F.col("days_since_moved_at").desc(), "id")
)
