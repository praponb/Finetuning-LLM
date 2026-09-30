from pyspark.sql import functions as F

prepared_warehouses = (
    df_warehouses
    .filter(F.length(F.trim(F.col("wh_code"))) >= 8)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.month(F.col("opened_on")).alias("opened_on_month"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    prepared_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

result = (
    with_counts
    .groupingSets(
        [["opened_on_month", "move_count", "city"], ["opened_on_month"], []],
        "opened_on_month", "move_count", "city",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("move_count"), 2).alias("stddev_move_count"),
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("move_count") / F.nullif(F.sum("opened_on_month"), F.lit(0)), 3).alias("move_count_to_opened_on_month_ratio"),
    )
    .select(
        "opened_on_month",
        "move_count",
        F.coalesce(F.col("city"), F.lit("ALL")).alias("city"),
        "gid",
        "stddev_move_count",
        "city_list",
        "move_count_to_opened_on_month_ratio",
    )
)
