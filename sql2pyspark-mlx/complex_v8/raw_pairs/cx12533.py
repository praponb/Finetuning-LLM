from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_plan_or_unknown = Window.partitionBy("plan_or_unknown")
w_all_by_signup_date_week = Window.orderBy("signup_date_week", "id")
w_all_by_signup_date_week_prio = Window.orderBy("signup_date_week", "id").rowsBetween(-1, -1)
w_all_by_signup_date_week_seq = Window.orderBy(F.col("signup_date_week_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_signup_date_week_seq_2 = Window.orderBy(F.col("signup_date_week_seq").desc())
w_all_by_signup_date_week_seq_3 = Window.orderBy(F.col("signup_date_week_seq").desc(), "id")
w_plan = Window.partitionBy("plan")
w_all_by_signup_date_week_seq_4 = Window.orderBy(F.col("signup_date_week_seq").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_listeners = (
    df_listeners
    .filter((F.col("birth_year") >= 1975) & (F.col("plan").isin("family", "student", "free")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
        F.length(F.trim(F.col("plan"))).alias("plan_len"),
    )
)

labeled = (
    scoped_listeners
    .select(
        "*",
        F.abs(F.col("plan_len") - 10).alias("plan_len_distance"),
        F.date_trunc("week", F.col("signup_date")).alias("signup_date_week"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "plan_or_unknown",
        "plan_len_distance",
        "signup_date_week",
        F.rank().over(w_all_by_signup_date).alias("signup_date_rank"),
        F.max("plan_len").over(w_plan_or_unknown).alias("plan_or_unknown_max_plan_len"),
        F.row_number().over(w_all_by_signup_date_week).alias("signup_date_week_seq"),
        F.sum("plan_len_distance").over(w_all_by_signup_date_week_prio).alias("plan_len_distance_sum_1_prior"),
    )
)

top_rows = (
    with_windows
    .filter((F.col("signup_date_rank") == 1) & (F.col("plan_or_unknown").isin("free", "student")))
)

ranked = (
    top_rows
    .select(
        "*",
        F.max("signup_date_week_seq").over(w_all_by_signup_date_week_seq).alias("max_signup_date_week_seq_so_far"),
        F.round(F.cume_dist().over(w_all_by_signup_date_week_seq_2), 3).alias("signup_date_week_seq_cume_dist"),
        F.rank().over(w_all_by_signup_date_week_seq_3).alias("signup_date_week_seq_rank"),
        F.round(F.col("plan_len_distance") / F.nullif(F.sum("plan_len_distance").over(w_plan), F.lit(0)), 4).alias("plan_len_distance_share_of_plan"),
        F.first("signup_date_week_seq").over(w_all_by_signup_date_week_seq_4).alias("first_signup_date_week_seq"),
    )
)

summary = (
    ranked
    .filter(F.col("plan_len_distance_share_of_plan") <= 0.6)
    .groupBy(F.date_trunc("month", "signup_date").alias("signup_date_month"))
    .agg(
        F.count(F.when(F.col("plan").isin("free", "premium", "student"), 1)).alias("n_plan_in_free_premium_student"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("plan")).alias("plan_list"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .filter(F.col("n_plan_in_free_premium_student") > 4)
    .select("signup_date_month", "n_plan_in_free_premium_student", "plan_list")
    .orderBy(F.col("signup_date_month").desc_nulls_first())
)
