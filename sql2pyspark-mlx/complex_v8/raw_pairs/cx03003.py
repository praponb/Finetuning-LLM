from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
        F.sum("qty").alias("total_move_qty"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.total_move_qty",
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
        F.round(F.col("qty_filled_2") / F.nullif(F.col("total_move_qty"), F.lit(0)), 3).alias("qty_filled_2_per_total_move_qty"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "qty_filled",
        "last_moved_at",
        "move_count",
        "total_move_qty",
        "operator_or_unknown",
        "qty_filled_2_per_total_move_qty",
        F.coalesce(F.col("qty_filled_2"), F.lit(0)).alias("qty_filled_2_filled"),
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
    .orderBy(F.col("move_type_prefix").asc())
)
