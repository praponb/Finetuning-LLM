from pyspark.sql import functions as F

prepared = (
    df_stock_moves
    .filter(F.col("qty") > 124)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

labeled = (
    prepared
    .select(
        "*",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("move_operator_count"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

with_children = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    with_children
    .filter(F.col("unit_price") < 363.6)
    .select(
        "id",
        "warehouse_id",
        "move_move_type",
        "move_unit_price",
        (F.col("move_unit_price") - F.col("unit_price")).alias("move_unit_price_minus_unit_price"),
    )
)
