from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id")
w_holder_id_by_end_date_full = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.month("start_date")).orderBy("start_date")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.month("start_date")).orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_start_date_moving_1 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_end_date_quartile_start_date = Window.partitionBy("end_date_quartile", F.year("start_date")).orderBy("premium_vs_holder_id_avg")
w_end_date_quartile_start_date_2 = Window.partitionBy("end_date_quartile", F.month("start_date")).orderBy("premium_share_of_holder_id", "id")
w_end_date_quartile = Window.partitionBy("end_date_quartile")
w_end_date_quartile_start_date_3 = Window.partitionBy("end_date_quartile", F.year("start_date")).orderBy("premium_vs_holder_id_avg", "id")

prepared = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.lead("premium").over(w_holder_id_by_end_date).alias("next_premium"),
        F.nth_value("premium", 2).over(w_holder_id_by_end_date_full).alias("second_premium"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
        F.round(F.percent_rank().over(w_holder_id_start_date_month_b), 3).alias("start_date_percent_rank"),
        F.min("premium").over(w_all_by_start_date_running).alias("min_premium_so_far"),
        F.round(F.avg("premium").over(w_holder_id_start_date_month_b_2), 2).alias("premium_avg_3"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.round(F.avg("premium").over(w_all_by_start_date_moving_1), 2).alias("premium_avg_2"),
        F.ntile(4).over(w_holder_id_by_end_date).alias("end_date_quartile"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "holder_id_min_premium",
        "premium_share_of_holder_id",
        "premium_vs_holder_id_avg",
        "holder_id_max_premium",
        "holder_id_sum_premium",
        "premium_avg_2",
        "end_date_quartile",
        F.round(F.cume_dist().over(w_end_date_quartile_start_date), 3).alias("premium_vs_holder_id_avg_cume_dist"),
        F.lag("min_premium_so_far").over(w_end_date_quartile_start_date_2).alias("prev_min_premium_so_far"),
        F.size(F.collect_set("holder_id").over(w_end_date_quartile)).alias("distinct_holder_id_in_end_date_quartile"),
        F.lag("next_premium", 3, 0).over(w_end_date_quartile_start_date_3).alias("prev3_next_premium"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "holder_id",
        "start_date",
        "holder_id_max_premium",
        "holder_id_sum_premium",
        "premium_avg_2",
        "end_date_quartile",
        "premium_vs_holder_id_avg_cume_dist",
        "prev3_next_premium",
        (F.floor(F.col("prev3_next_premium") / 1000) * 1000).alias("prev3_next_premium_bucket"),
        F.coalesce(F.col("prev_min_premium_so_far"), F.lit(0.0)).alias("prev_min_premium_so_far_filled"),
    )
    .orderBy(F.col("prev3_next_premium").desc())
)
