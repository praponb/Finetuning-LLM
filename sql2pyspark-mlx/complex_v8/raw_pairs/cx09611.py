from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_count_opened_on_year = Window.partitionBy("move_count", F.year("opened_on"))
w_all_by_move_count = Window.orderBy("move_count", "id")
w_all_by_move_count_prior_3 = Window.orderBy("move_count", "id").rowsBetween(-3, -1)
w_move_count_2_by_move_count_2 = Window.partitionBy("move_count_2").orderBy("move_count_2")
w_move_count_2_by_move_count_2_2 = Window.partitionBy("move_count_2").orderBy("move_count_2", "id")
w_move_count_2_by_move_count_2_3 = Window.partitionBy("move_count_2").orderBy("move_count_2", "id").rowsBetween(-2, -1)

valid_warehouses = (
    df_warehouses
    .filter(~F.col("city").isin("Singapore", "Kuala Lumpur"))
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
    .agg(F.count("*").alias("move_count"), F.count("*").alias("move_count_2"))
)

with_counts = (
    valid_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
    )
)

with_trends = (
    with_counts
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        "move_count",
        "move_count_2",
        F.size(F.collect_set("city").over(w_move_count_opened_on_year)).alias("distinct_city_in_move_count_opened_on_year"),
        F.round(F.cume_dist().over(w_all_by_move_count), 3).alias("move_count_cume_dist"),
        F.max("move_count").over(w_all_by_move_count_prior_3).alias("move_count_max_3_prior"),
        F.row_number().over(w_all_by_move_count).alias("move_count_seq"),
    )
)

leaders = (
    with_trends
    .filter(F.col("move_count_seq") == 1)
    .drop("move_count_seq")
)

sequenced = (
    leaders
    .select(
        "id",
        "wh_code",
        "capacity",
        "move_count",
        "distinct_city_in_move_count_opened_on_year",
        "move_count_max_3_prior",
        F.rank().over(w_move_count_2_by_move_count_2).alias("move_count_2_rank"),
        F.ntile(3).over(w_move_count_2_by_move_count_2_2).alias("move_count_2_tercile"),
        F.round(F.avg("move_count_2").over(w_move_count_2_by_move_count_2_3), 2).alias("move_count_2_avg_2_prior"),
    )
)

result = (
    sequenced
    .rollup("move_count_2_rank", "move_count_2_tercile")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("wh_code"))).like("K%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_wh_code_like_k"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("move_count_2_rank").asc_nulls_last(),
        F.col("move_count_2_tercile").asc_nulls_last(),
    )
)
