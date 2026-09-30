from pyspark.sql import functions as F

base = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
    )
)

exploded_detail = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator_len",
        F.col("st.id").alias("move_id"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.qty").alias("move_qty"),
    )
)

without_matches = (
    exploded_detail.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.move_sku_id")) & (F.col("sk.category") == "Apparel"), "left_semi")
)

with_details_move = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .filter(F.col("st.qty") <= 349)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator_len",
        "s.move_id",
        "s.move_moved_at",
        "s.move_operator",
        "s.move_sku_id",
        "s.move_qty",
        F.col("st.qty").alias("move_qty_2"),
    )
)

result = (
    with_details_move
    .groupBy("operator_len", "sku_id")
    .pivot("move_operator", ["op_ann", "op_eve"])
    .agg(F.avg("move_qty_2"))
)
