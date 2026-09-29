from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_children_moves = (
    prepared_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 341.5), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.days_since_moved_at",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    with_children_moves
    .rollup("move_operator", "move_id", "move_type")
    .agg(
        F.min("days_since_moved_at").alias("min_days_since_moved_at"),
        F.round(F.stddev("move_qty"), 2).alias("stddev_move_qty"),
    )
    .orderBy(
        F.col("move_operator").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
