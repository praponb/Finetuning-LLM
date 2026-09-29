from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count_by_capacity = Window.partitionBy("move_count").orderBy(F.col("capacity").desc_nulls_first(), "id")
w_city_opened_on_month = Window.partitionBy("city", F.month("opened_on"))
w_opened_on_year_by_move_count = Window.partitionBy(F.year("opened_on")).orderBy(F.col("move_count").desc())

prepared = (
    df_warehouses
    .filter((F.col("wh_code").isNotNull()) & (F.col("capacity") <= 11768))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.sum("qty").alias("total_move_qty"), F.count("*").alias("move_count"))
)

profiled = (
    prepared.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

ranked = (
    profiled
    .select(
        "id",
        "city",
        "capacity",
        "total_move_qty",
        "move_count",
        F.lead("capacity").over(w_move_count_by_capacity).alias("next_capacity"),
        F.lag("capacity").over(w_move_count_by_capacity).alias("prev_capacity"),
        F.count(F.lit(1)).over(w_city_opened_on_month).alias("rows_in_city_opened_on_month"),
        F.sum("move_count").over(w_city_opened_on_month).alias("city_opened_on_month_sum_move_count"),
        F.round(F.cume_dist().over(w_opened_on_year_by_move_count), 3).alias("move_count_cume_dist"),
    )
)

move_stats_2 = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_move_unit_price"))
)

with_counts = (
    ranked.alias("s")
    .join(move_stats_2.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.avg_move_unit_price")
)

summary = (
    with_counts
    .filter(F.col("next_capacity") >= 2614)
    .groupBy("city")
    .agg(
        F.array_sort(F.collect_set("city")).alias("city_list"),
        F.sum(F.when(F.col("city").isNotNull(), 1).otherwise(0)).alias("n_city_is_not_null"),
        F.countDistinct("city").alias("distinct_citys"),
        F.count(F.when(F.col("city") != "Singapore", 1)).alias("n_city_singapore"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

screened = (
    summary
    .crossJoin(summary.agg(F.max("distinct_citys").alias("overall_max_distinct_citys")))
    .filter(F.col("distinct_citys") < F.col("overall_max_distinct_citys"))
    .drop("overall_max_distinct_citys")
)

result = (
    screened
    .filter(F.col("n_city_is_not_null") > 2)
    .select(
        "city",
        "city_list",
        "n_city_is_not_null",
        "distinct_citys",
        "n_city_singapore",
        "cnt",
        "distinct_ids",
        F.least(F.col("n_city_singapore"), F.col("distinct_citys")).alias("least_n_city_singapore_distinct_citys"),
    )
)
