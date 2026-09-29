from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_chan")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.abs(F.col("unit_price") - 313.8).alias("unit_price_distance"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

profiled = (
    prepared_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

without_matches = (
    profiled.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.moved_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")) & (F.col("st.unit_price") < 363.6), "left_semi")
)

result = (
    without_matches
    .rollup("move_type", "warehouse_id", "move_count")
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("move_type") == "OUTBOUND", 1)).alias("n_move_type_outbound"),
        F.round(F.stddev("unit_price_distance"), 2).alias("stddev_unit_price_distance"),
        F.countDistinct("operator").alias("distinct_operators"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "warehouse_id",
        "move_count",
        "gid",
        "n_move_type_outbound",
        "stddev_unit_price_distance",
        "distinct_operators",
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("move_count").asc_nulls_last(),
    )
)
