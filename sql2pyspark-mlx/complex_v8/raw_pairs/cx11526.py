from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_all_by_signup_date_ties = Window.orderBy(F.col("signup_date").desc())
w_all_by_signup_date_full = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_by_signup_date_ties = Window.partitionBy("plan").orderBy(F.col("signup_date").asc_nulls_last())
w_all_by_signup_date_plus_7d = Window.orderBy("signup_date_plus_7d", "id")
w_all_by_signup_date_ties_2 = Window.orderBy("signup_date")
w_all_by_signup_date_2 = Window.orderBy(F.col("signup_date").desc(), "id")
w_all_by_second_signup_date_pl = Window.orderBy("second_signup_date_plus_7d", "id")
w_signup_date_rank = Window.partitionBy("signup_date_rank")
w_signup_date_dense_rank_3_sig = Window.partitionBy("signup_date_dense_rank_3", "signup_date_dense_rank_2")
w_all_by_signup_date_running = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_listeners = (
    df_listeners
    .filter((~F.col("plan").isin("student", "family")) & (F.lower(F.col("username")).like("%fa%")))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "interests",
        F.date_add(F.col("signup_date"), 7).alias("signup_date_plus_7d"),
    )
)

with_windows = (
    filtered_listeners
    .select(
        "id",
        "plan",
        "signup_date",
        "interests",
        "signup_date_plus_7d",
        F.datediff(F.col("signup_date_plus_7d"), F.lag("signup_date_plus_7d").over(w_all_by_signup_date)).alias("days_since_prev_signup_date_plus_7d"),
        F.rank().over(w_all_by_signup_date_ties).alias("signup_date_rank"),
        F.nth_value("signup_date", 2).over(w_all_by_signup_date_full).alias("second_signup_date"),
        F.round(F.cume_dist().over(w_all_by_signup_date), 3).alias("signup_date_cume_dist"),
        F.nth_value("signup_date_plus_7d", 2).over(w_all_by_signup_date_full).alias("second_signup_date_plus_7d"),
        F.dense_rank().over(w_plan_by_signup_date_ties).alias("signup_date_dense_rank"),
        F.lag("signup_date_plus_7d").over(w_all_by_signup_date_plus_7d).alias("prev_signup_date_plus_7d"),
        F.row_number().over(w_all_by_signup_date_plus_7d).alias("seq_no"),
        F.dense_rank().over(w_all_by_signup_date_ties_2).alias("signup_date_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_signup_date_plus_7d), 3).alias("signup_date_plus_7d_percent_rank"),
        F.dense_rank().over(w_all_by_signup_date_2).alias("signup_date_dense_rank_3"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

with_trends = (
    top_per_group
    .select(
        "*",
        F.dense_rank().over(w_all_by_signup_date_ties_2).alias("signup_date_dense_rank_4"),
        F.lead("signup_date_plus_7d", 3).over(w_all_by_second_signup_date_pl).alias("next3_signup_date_plus_7d"),
        F.round(F.col("signup_date_cume_dist") - F.avg("signup_date_cume_dist").over(w_signup_date_rank), 2).alias("signup_date_cume_dist_vs_signup_date_rank_avg"),
        F.max("signup_date_dense_rank_3").over(w_signup_date_dense_rank_3_sig).alias("signup_date_dense_rank_3_signup_date_dense_rank_"),
        F.max("signup_date_cume_dist").over(w_all_by_signup_date_running).alias("max_signup_date_cume_dist_so_far"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "signup_date",
        "interests",
        "signup_date_plus_7d",
        "days_since_prev_signup_date_plus_7d",
        "signup_date_rank",
        "second_signup_date",
        "signup_date_cume_dist",
        "second_signup_date_plus_7d",
        "signup_date_dense_rank",
        "signup_date_dense_rank_2",
        "signup_date_plus_7d_percent_rank",
        "signup_date_dense_rank_3",
        "signup_date_cume_dist_vs_signup_date_rank_avg",
        "signup_date_dense_rank_3_signup_date_dense_rank_",
        "max_signup_date_cume_dist_so_far",
        F.round(F.col("signup_date_rank") / F.nullif(F.col("signup_date_dense_rank_4"), F.lit(0)), 3).alias("signup_date_rank_per_signup_date_dense_rank_4"),
    )
    .orderBy("signup_date_dense_rank")
)
