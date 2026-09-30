from pyspark.sql import functions as F

recent = (
    df_warehouses
    .filter((F.col("capacity") >= 2614) & (F.col("opened_on") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

with_counts = (
    recent.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.last_moved_at",
        "st.last_moved_at_2",
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count(F.when(F.col("move_type").isNotNull(), 1)).alias("move_move_type_count"),
        F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"),
        F.max("moved_at").alias("last_moved_at_3"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
        "st.avg_move_unit_price",
        "st.last_moved_at_3",
    )
)

result = (
    profiled
    .groupingSets(
        [["move_count", "move_move_type_count"], ["move_count"], ["move_move_type_count"]],
        "move_count", "move_move_type_count",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
