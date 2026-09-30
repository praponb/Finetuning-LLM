from pyspark.sql import functions as F

recent_moves = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-02-12 14:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.year(F.col("moved_at")).alias("moved_at_year"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    recent_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.avg_move_unit_price", "st.last_moved_at")
)

annotated = (
    with_stats
    .select(
        "*",
        F.year(F.col("moved_at")).alias("moved_at_year_2"),
        F.abs(F.col("unit_price") - 313.8).alias("unit_price_distance"),
        F.dayofweek(F.col("last_moved_at")).alias("last_moved_at_dayofweek"),
    )
)

labeled = annotated.select("*", F.to_date(F.col("moved_at")).alias("moved_at_date"))

without_matches = (
    labeled.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.unit_cost") > 158.7) & (~F.array_contains(F.col("sk.labels"), "hazmat")), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["sku_id", "moved_at_year", "moved_at_year_2"], ["sku_id"], ["moved_at_year"]],
        "sku_id", "moved_at_year", "moved_at_year_2",
    )
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.max("unit_price_distance").alias("max_unit_price_distance"),
        F.percentile_approx("avg_move_unit_price", 0.5).alias("median_avg_move_unit_price"),
        F.round(F.sum("qty") / F.nullif(F.sum("moved_at_year"), F.lit(0)), 3).alias("qty_to_moved_at_year_ratio"),
    )
    .orderBy(
        F.col("sku_id").asc_nulls_last(),
        F.col("moved_at_year").asc_nulls_last(),
        F.col("moved_at_year_2").asc_nulls_last(),
    )
)
