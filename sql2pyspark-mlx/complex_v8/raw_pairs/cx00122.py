from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date_full = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_plan = Window.partitionBy("plan")
w_all_by_birth_year = Window.orderBy("birth_year", "id")
w_all_by_signup_date_ties = Window.orderBy(F.col("signup_date").asc_nulls_last())
w_all_by_signup_date_running = Window.orderBy(F.col("signup_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_signup_date_2 = Window.orderBy(F.col("signup_date").asc_nulls_last(), "id")
w_signup_date_dense_rank_signu = Window.partitionBy("signup_date_dense_rank", F.date_trunc("month", "signup_date"))
w_all_by_signup_date_dense_ran = Window.orderBy(F.col("signup_date_dense_rank").desc())
w_signup_date_dense_rank_by_ro = Window.partitionBy("signup_date_dense_rank").orderBy("rows_in_plan", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_dense_rank_by_ro_2 = Window.partitionBy("signup_date_dense_rank").orderBy("rows_in_plan", "id")
w_all_by_rows_in_plan = Window.orderBy("rows_in_plan", "id")
w_all_by_signup_date_dense_ran_2 = Window.orderBy(F.col("signup_date_dense_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = df_listeners.filter(F.col("plan") == "free")

sequenced = (
    prepared
    .select(
        "*",
        F.last("signup_date").over(w_all_by_signup_date_full).alias("last_signup_date"),
        F.round(F.cume_dist().over(w_all_by_signup_date), 3).alias("signup_date_cume_dist"),
        F.count(F.lit(1)).over(w_plan).alias("rows_in_plan"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_plan), F.lit(0)), 4).alias("birth_year_share_of_plan"),
        F.ntile(5).over(w_all_by_birth_year).alias("birth_year_quintile"),
        F.round(F.col("birth_year") - F.avg("birth_year").over(w_plan), 2).alias("birth_year_vs_plan_avg"),
        F.dense_rank().over(w_all_by_signup_date_ties).alias("signup_date_dense_rank"),
        F.last("birth_year", ignorenulls=True).over(w_all_by_signup_date_running).alias("last_known_birth_year"),
        F.lead("signup_date", 2).over(w_all_by_signup_date_2).alias("next2_signup_date"),
        F.max("birth_year").over(w_plan).alias("plan_max_birth_year"),
        F.nth_value("signup_date", 2).over(w_all_by_signup_date_full).alias("second_signup_date"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("plan").over(w_signup_date_dense_rank_signu)).alias("distinct_plan_in_signup_date_dense_rank_signup_d"),
        F.rank().over(w_all_by_signup_date_dense_ran).alias("signup_date_dense_rank_rank"),
        F.nth_value("birth_year_quintile", 2).over(w_signup_date_dense_rank_by_ro).alias("second_birth_year_quintile"),
        F.lead("signup_date_dense_rank", 2).over(w_signup_date_dense_rank_by_ro_2).alias("next2_signup_date_dense_rank"),
        F.row_number().over(w_signup_date_dense_rank_by_ro_2).alias("rows_in_plan_seq"),
        F.datediff(F.col("last_signup_date"), F.lag("last_signup_date").over(w_all_by_rows_in_plan)).alias("days_since_prev_last_signup_date"),
        F.nth_value("last_signup_date", 2).over(w_all_by_signup_date_dense_ran_2).alias("second_last_signup_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "birth_year",
        "interests",
        "last_signup_date",
        "signup_date_cume_dist",
        "rows_in_plan",
        "birth_year_share_of_plan",
        "birth_year_quintile",
        "birth_year_vs_plan_avg",
        "signup_date_dense_rank",
        "second_signup_date",
        "distinct_plan_in_signup_date_dense_rank_signup_d",
        "signup_date_dense_rank_rank",
        "second_birth_year_quintile",
        "second_last_signup_date",
        (F.floor(F.col("rows_in_plan_seq") / 2) * 2).alias("rows_in_plan_seq_bucket"),
        F.greatest(F.col("signup_date_dense_rank"), F.col("rows_in_plan_seq")).alias("greatest_signup_date_dense_rank_rows_in_plan_seq"),
    )
    .orderBy(F.col("birth_year_share_of_plan").desc())
)
