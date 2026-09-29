from pyspark.sql import functions as F

filtered_moves = (
    df_stock_moves
    .filter(F.col("moved_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

qualified = (
    filtered_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type") == "ADJUST"), "left_semi")
)

line_level = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.unit_price_adj",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    line_level
    .filter(F.col("warehouse_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price_adj",
        "move_id",
        "move_moved_at",
        "move_sku_id",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.col("move_qty") - F.col("unit_price")).alias("move_qty_minus_unit_price"),
    )
    .orderBy(
        F.col("move_sku_id").desc(),
        F.col("qty_bucket").asc_nulls_last(),
        "id",
        "move_id",
    )
    .limit(10)
)
