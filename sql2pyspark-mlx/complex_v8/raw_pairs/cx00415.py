from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
)

without_matches = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date"))
)

labeled = (
    without_matches
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_per_unit_price",
        F.when(F.col("qty_per_unit_price") >= 7.5, "HIGH").when(F.col("qty_per_unit_price") >= 4.0, "MEDIUM").otherwise("LOW").alias("qty_per_unit_price_tier"),
    )
)

result = (
    labeled
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "qty_per_unit_price",
        "qty_per_unit_price_tier",
    )
    .orderBy(F.col("id").desc(), "operator")
)
