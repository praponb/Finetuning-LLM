from pyspark.sql import functions as F

scoped = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isin("op_eve", "op_chan")) & (F.col("st.warehouse_id") >= 5), "left_semi")
    .filter(F.col("sku_id").isNotNull())
)

with_children = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.operator_slug",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

with_children_2 = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        "s.operator_slug",
        "s.move_id",
        "s.move_move_type",
        "s.move_qty",
        "s.move_sku_id",
        F.col("st.id").alias("move_id_2"),
        "st.move_type",
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty_2"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    with_children_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_id",
        "move_move_type",
        "move_qty",
        "move_sku_id",
        "move_type",
        "move_operator",
        "move_qty_2",
        "move_moved_at",
    )
)
