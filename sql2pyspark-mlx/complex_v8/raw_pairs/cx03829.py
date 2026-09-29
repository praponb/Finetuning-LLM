from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        (F.col("moved_at") + F.expr("INTERVAL 30 MINUTES")).alias("moved_at_plus_30m"),
        F.substring(F.trim(F.col("operator")), 1, 4).alias("operator_prefix"),
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("qty").alias("min_move_qty"), F.count("*").alias("move_count"))
)

with_stats = (
    scoped_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("move_type").isin("ADJUST", "TRANSFER", "OUTBOUND"), 1)).alias("move_move_type_count"),
        F.sum("unit_price").alias("total_move_unit_price"),
    )
)

profiled = (
    with_stats.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.total_move_unit_price",
    )
)

enriched = (
    profiled
    .filter(F.col("moved_at_dayofweek") < 6)
    .select(
        "*",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.when(F.col("operator").isin("op_chan", "op_bee"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.round((F.unix_timestamp(F.col("moved_at")) - F.unix_timestamp(F.col("moved_at_plus_30m"))) / 60, 1).alias("minutes_moved_at_plus_30m_to_moved_at"),
    )
)

grouped = (
    enriched
    .groupBy("moved_at_dayofweek")
    .agg(
        F.min("min_move_qty").alias("min_min_move_qty"),
        F.sum("moved_at_dayofweek").alias("total_moved_at_dayofweek"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("move_move_type_count").alias("total_move_move_type_count"),
        F.min("move_count").alias("min_move_count"),
        F.sum("move_count").alias("total_move_count"),
        F.avg("move_count").alias("avg_move_count_check"),
    )
    .filter(F.col("avg_move_count_check") > 2)
    .drop("avg_move_count_check")
)

enriched_2 = (
    grouped
    .select(
        "*",
        F.abs(F.col("total_move_count") - 24).alias("total_move_count_distance"),
        (F.col("total_moved_at_dayofweek") - F.col("min_min_move_qty")).alias("total_moved_at_dayofweek_minus_min_min_move_qty"),
    )
)

labeled = (
    enriched_2
    .select(
        "*",
        F.greatest(F.col("min_min_move_qty"), F.col("total_move_count")).alias("greatest_min_min_move_qty_total_move_count"),
        F.abs(F.col("moved_at_dayofweek") - 3).alias("moved_at_dayofweek_distance"),
    )
)

result = (
    labeled
    .select(
        "moved_at_dayofweek",
        "min_min_move_qty",
        "distinct_ids",
        "total_move_move_type_count",
        "min_move_count",
        "total_move_count",
        "total_move_count_distance",
        "total_moved_at_dayofweek_minus_min_min_move_qty",
        "moved_at_dayofweek_distance",
        F.when(F.col("distinct_ids") >= 8, "A").when(F.col("distinct_ids") >= 4, "B").otherwise("C").alias("distinct_ids_tier"),
    )
    .orderBy(
        F.col("moved_at_dayofweek_distance").desc(),
        F.col("total_move_move_type_count").desc(),
    )
)
