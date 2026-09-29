from pyspark.sql import functions as F

base_warehouses = (
    df_warehouses
    .filter((F.col("opened_on").between(F.lit("2024-04-13").cast("date"), F.lit("2025-12-17").cast("date"))) & (F.col("capacity") <= 11768))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.coalesce(F.col("city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    base_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count_2"))
)

with_counts = (
    summarized_rows.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

result = (
    with_counts
    .groupingSets(
        [["move_count", "city"], ["move_count"], ["city"], []],
        "move_count", "city",
    )
    .agg(
        F.sum("move_count_2").alias("total_move_count_2"),
        F.sum("capacity").alias("total_capacity"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("move_count").asc_nulls_last(), F.col("city").asc_nulls_last())
)
