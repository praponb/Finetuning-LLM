from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_opened_on_dayofweek = Window.partitionBy("city", "opened_on_dayofweek")
w_opened_on_dayofweek_opened_o = Window.partitionBy("opened_on_dayofweek", F.month("opened_on")).orderBy("capacity", "id")
w_opened_on_dayofweek_by_capac = Window.partitionBy("opened_on_dayofweek").orderBy("capacity", "id")
w_city_by_opened_on_dayofweek = Window.partitionBy("city").orderBy("opened_on_dayofweek", "id")
w_city_by_opened_on_dayofweek_2 = Window.partitionBy("city").orderBy("opened_on_dayofweek")
w_city_by_opened_on_dayofweek_3 = Window.partitionBy("city").orderBy("opened_on_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_city_by_opened_on_dayofweek_4 = Window.partitionBy("city").orderBy("opened_on_dayofweek", "id").rowsBetween(-2, -1)
w_opened_on_dayofweek_by_capac_2 = Window.partitionBy("opened_on_dayofweek").orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_capacity_rank_by_prev2_capac = Window.partitionBy("capacity_rank").orderBy("prev2_capacity", "id")
w_opened_on_dayofweek_rank_ope = Window.partitionBy("opened_on_dayofweek_rank", F.date_trunc("month", "opened_on")).orderBy("last_opened_on_dayofweek", "id").rowsBetween(-3, -1)
w_opened_on_dayofweek_rank_ope_2 = Window.partitionBy("opened_on_dayofweek_rank", F.date_trunc("month", "opened_on")).orderBy("last_opened_on_dayofweek", "id")
w_opened_on_year_first_opened = Window.partitionBy(F.year("opened_on"), "first_opened_on_dayofweek")
w_first_opened_on_dayofweek_by = Window.partitionBy("first_opened_on_dayofweek").orderBy(F.col("prev2_capacity").asc_nulls_last(), "id")

filtered = (
    df_warehouses
    .filter((F.col("city").isNotNull()) & (F.length(F.trim(F.col("wh_code"))) >= 8))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
        F.substring(F.trim(F.col("city")), 1, 3).alias("city_prefix"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "wh_code",
        "opened_on",
        "opened_on_dayofweek",
        F.min("capacity").over(w_city_opened_on_dayofweek).alias("city_opened_on_dayofweek_min_capacity"),
        F.lag("capacity", 2, 0).over(w_opened_on_dayofweek_opened_o).alias("prev2_capacity"),
        F.rank().over(w_opened_on_dayofweek_by_capac).alias("capacity_rank"),
        F.round(F.percent_rank().over(w_city_by_opened_on_dayofweek), 3).alias("opened_on_dayofweek_percent_rank"),
        F.round(F.percent_rank().over(w_city_by_opened_on_dayofweek_2), 3).alias("opened_on_dayofweek_percent_rank_2"),
        F.last("opened_on_dayofweek").over(w_city_by_opened_on_dayofweek_3).alias("last_opened_on_dayofweek"),
        F.rank().over(w_city_by_opened_on_dayofweek).alias("opened_on_dayofweek_rank"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_city_opened_on_dayofweek), 2).alias("capacity_vs_city_opened_on_dayofweek_avg"),
        F.datediff(F.col("opened_on"), F.lag("opened_on").over(w_opened_on_dayofweek_opened_o)).alias("days_since_prev_opened_on"),
        F.first("opened_on_dayofweek").over(w_city_by_opened_on_dayofweek_3).alias("first_opened_on_dayofweek"),
        F.round(F.avg("opened_on_dayofweek").over(w_city_by_opened_on_dayofweek_4), 2).alias("opened_on_dayofweek_avg_2_prior"),
        F.round(F.cume_dist().over(w_opened_on_dayofweek_by_capac), 3).alias("capacity_cume_dist"),
        F.count(F.lit(1)).over(w_opened_on_dayofweek_by_capac_2).alias("running_count"),
    )
)

top_per_group = with_trends.filter(F.col("capacity_rank") == 1)

ranked = (
    top_per_group
    .select(
        "*",
        F.lead("opened_on", 2).over(w_capacity_rank_by_prev2_capac).alias("next2_opened_on"),
        F.max("capacity_cume_dist").over(w_opened_on_dayofweek_rank_ope).alias("capacity_cume_dist_max_3_prior"),
        F.lag("first_opened_on_dayofweek").over(w_opened_on_dayofweek_rank_ope_2).alias("prev_first_opened_on_dayofweek"),
        F.count(F.lit(1)).over(w_opened_on_year_first_opened).alias("rows_in_opened_on_year_first_opened_on_dayofweek"),
        F.rank().over(w_first_opened_on_dayofweek_by).alias("prev2_capacity_rank"),
    )
)

result = (
    ranked
    .filter(F.col("capacity_rank") <= 9)
    .select(
        "id",
        "wh_code",
        "opened_on_dayofweek",
        "city_opened_on_dayofweek_min_capacity",
        "prev2_capacity",
        "capacity_rank",
        "opened_on_dayofweek_percent_rank",
        "opened_on_dayofweek_percent_rank_2",
        "last_opened_on_dayofweek",
        "opened_on_dayofweek_rank",
        "capacity_vs_city_opened_on_dayofweek_avg",
        "days_since_prev_opened_on",
        "opened_on_dayofweek_avg_2_prior",
        "capacity_cume_dist",
        "running_count",
        "next2_opened_on",
        "capacity_cume_dist_max_3_prior",
        "prev2_capacity_rank",
    )
    .orderBy(F.col("days_since_prev_opened_on").desc())
)
