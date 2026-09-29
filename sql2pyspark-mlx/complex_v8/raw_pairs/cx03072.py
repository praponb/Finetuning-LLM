from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_year = Window.partitionBy(F.year("opened_on"))
w_city_by_capacity_moving_3 = Window.partitionBy("city").orderBy("capacity", "id").rowsBetween(-3, Window.currentRow)
w_city_by_capacity = Window.partitionBy("city").orderBy(F.col("capacity").desc(), "id")
w_all_by_capacity_share_of_ope = Window.orderBy(F.col("capacity_share_of_opened_on_year").desc(), "id")
w_all_by_opened_on_year_max_ca = Window.orderBy(F.col("opened_on_year_max_capacity").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_on_year_max_ca_2 = Window.orderBy(F.col("opened_on_year_max_capacity").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_capacity_avg_4_moving = Window.orderBy("capacity_avg_4", "id").rowsBetween(-2, Window.currentRow)
w_all_by_capacity_avg_4_full = Window.orderBy("capacity_avg_4", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_capacity_share_of_opened_on = Window.partitionBy("capacity_share_of_opened_on_year_seq").orderBy(F.col("capacity").desc(), "id")
w_capacity_share_of_opened_on_2 = Window.partitionBy("capacity_share_of_opened_on_year_seq")
w_all_by_capacity_share_of_ope_2 = Window.orderBy(F.col("capacity_share_of_opened_on_year").asc_nulls_last())
w_capacity_rank = Window.partitionBy("capacity_rank")
w_capacity_rank_capacity_share = Window.partitionBy("capacity_rank", "capacity_share_of_opened_on_year_seq").orderBy(F.col("capacity_cume_dist_avg_4").desc(), "id").rowsBetween(-1, Window.currentRow)
w_capacity_share_of_opened_on_3 = Window.partitionBy("capacity_share_of_opened_on_year_seq", "capacity_share_of_opened_on_year_dense_rank").orderBy(F.col("first_opened_on_year_max_capacity").desc(), "id").rowsBetween(-3, Window.currentRow)
w_capacity_rank_capacity_share_2 = Window.partitionBy("capacity_rank", "capacity_share_of_opened_on_year_seq").orderBy("capacity_avg_3_share_of_capacity_share_of_opened", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_warehouses
    .filter(F.col("opened_on") >= F.lit("2024-11-25").cast("date"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.upper(F.trim(F.col("city"))).alias("city_clean"),
    )
)

ranked = (
    base
    .select(
        "id",
        "wh_code",
        "capacity",
        "city_clean",
        F.round(F.col("capacity") / F.nullif(F.sum("capacity").over(w_opened_on_year), F.lit(0)), 4).alias("capacity_share_of_opened_on_year"),
        F.round(F.avg("capacity").over(w_city_by_capacity_moving_3), 2).alias("capacity_avg_4"),
        F.round(F.cume_dist().over(w_city_by_capacity), 3).alias("capacity_cume_dist"),
        F.max("capacity").over(w_opened_on_year).alias("opened_on_year_max_capacity"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "capacity",
        "capacity_share_of_opened_on_year",
        F.row_number().over(w_all_by_capacity_share_of_ope).alias("capacity_share_of_opened_on_year_seq"),
        F.sum("capacity").over(w_all_by_opened_on_year_max_ca).alias("running_capacity"),
        F.round(F.avg("capacity_cume_dist").over(w_all_by_opened_on_year_max_ca_2), 2).alias("capacity_cume_dist_avg_4"),
        F.round(F.avg("capacity").over(w_all_by_capacity_avg_4_moving), 2).alias("capacity_avg_3"),
        F.first("opened_on_year_max_capacity").over(w_all_by_capacity_avg_4_full).alias("first_opened_on_year_max_capacity"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.rank().over(w_capacity_share_of_opened_on).alias("capacity_rank"),
        F.round(F.col("capacity_avg_3") / F.nullif(F.sum("capacity_avg_3").over(w_capacity_share_of_opened_on_2), F.lit(0)), 4).alias("capacity_avg_3_share_of_capacity_share_of_opened"),
        F.dense_rank().over(w_all_by_capacity_share_of_ope_2).alias("capacity_share_of_opened_on_year_dense_rank"),
    )
)

with_windows_2 = (
    with_trends
    .select(
        "*",
        F.round(F.avg("capacity_cume_dist_avg_4").over(w_capacity_rank_capacity_share), 2).alias("capacity_cume_dist_avg_4_avg_2"),
        F.round(F.avg("capacity_avg_3").over(w_capacity_share_of_opened_on_3), 2).alias("capacity_avg_3_avg_4"),
        F.nth_value("capacity_avg_3", 2).over(w_capacity_rank_capacity_share_2).alias("second_capacity_avg_3"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "capacity",
        "capacity_share_of_opened_on_year_seq",
        "running_capacity",
        "capacity_avg_3_share_of_capacity_share_of_opened",
        "capacity_cume_dist_avg_4_avg_2",
        "second_capacity_avg_3",
        (F.col("capacity_avg_3_avg_4") - F.col("second_capacity_avg_3")).alias("capacity_avg_3_avg_4_minus_second_capacity_avg_3"),
        F.round(F.col("capacity_rank") / F.nullif(F.col("capacity_cume_dist_avg_4"), F.lit(0)), 3).alias("capacity_rank_per_capacity_cume_dist_avg_4"),
    )
    .orderBy("capacity_rank_per_capacity_cume_dist_avg_4", "capacity")
)
