from pyspark.sql import functions as F

recent = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket_2"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.sum("qty").alias("total_move_qty"),
    )
)

with_stats = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        "st.total_move_qty",
    )
)

enriched = (
    with_stats
    .filter(F.col("move_count") > 1)
    .select(
        "*",
        F.hour(F.col("moved_at_plus_1d")).alias("moved_at_plus_1d_hour"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
        F.round(F.col("total_move_qty") / F.nullif(F.col("move_count"), F.lit(0)), 3).alias("total_move_qty_per_move_count"),
    )
)

result = (
    enriched
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "qty_bucket_2",
        "move_count",
        "move_count_2",
        "total_move_qty",
        "moved_at_plus_1d_hour",
        "qty_filled",
        "total_move_qty_per_move_count",
    )
    .orderBy(
        F.col("unit_price").desc(),
        F.col("total_move_qty_per_move_count").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
