from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 215.4) & (F.col("qty") < 314))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
    )
)

without_matches = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.warehouse_id") >= 5) & (F.col("st.operator").isin("op_dew", "op_bee")), "left_semi")
)

detail_rows_moves = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

labeled = (
    detail_rows_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_id",
        "move_move_type",
        "move_qty",
        "move_moved_at",
        (F.floor(F.col("move_qty") / 100) * 100).alias("move_qty_bucket"),
        F.coalesce(F.col("move_qty"), F.lit(0)).alias("move_qty_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "move_id",
        "move_qty",
        "move_qty_filled",
    )
    .unpivot(
        ids=["id", "move_id"],
        values=["move_qty", "move_qty_filled"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
