from pyspark.sql import functions as F

valid_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 2) & (~F.col("move_type").isin("INBOUND", "TRANSFER")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
    )
)

derived = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_dayofweek",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
        F.round(F.col("unit_price") * 1.5, 2).alias("unit_price_adj"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

profiled = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.first_moved_at")
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_counts = (
    profiled.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

enriched = (
    with_counts
    .filter(F.col("qty") >= 112)
    .select(
        "*",
        F.greatest(F.col("unit_price_adj"), F.col("move_count")).alias("greatest_unit_price_adj_move_count"),
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
    )
)

derived_2 = (
    enriched
    .select(
        "id",
        "qty",
        "operator",
        "unit_price",
        "moved_at_dayofweek",
        "greatest_qty_unit_price",
        "unit_price_adj",
        "move_count",
        "move_count_2",
        "greatest_unit_price_adj_move_count",
        "moved_at_hour",
        "moved_at_week",
        F.substring(F.trim(F.col("move_type")), 1, 4).alias("move_type_prefix"),
    )
)

operator_stats = (
    derived_2
    .groupBy("operator")
    .agg(F.percentile_approx("greatest_unit_price_adj_move_count", 0.5).alias("median_greatest_unit_price_adj_move_count"))
)

vs_group = (
    derived_2.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.greatest_unit_price_adj_move_count") >= F.col("g.median_greatest_unit_price_adj_move_count"))
    .select(
        "s.*",
        "g.median_greatest_unit_price_adj_move_count",
        F.round(F.col("s.greatest_unit_price_adj_move_count") - F.col("g.median_greatest_unit_price_adj_move_count"), 2).alias("greatest_unit_price_adj_move_count_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("move_count_2") <= 5)
    .select(
        "id",
        "qty",
        "operator",
        "moved_at_dayofweek",
        "unit_price_adj",
        "move_count",
        "move_count_2",
        "greatest_unit_price_adj_move_count",
        "moved_at_hour",
        "moved_at_week",
        "move_type_prefix",
        "median_greatest_unit_price_adj_move_count",
    )
)
