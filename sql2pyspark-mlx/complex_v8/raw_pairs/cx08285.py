from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_by_birth_year_bucket = Window.partitionBy("plan").orderBy("birth_year_bucket", "id")
w_plan_country = Window.partitionBy("plan", "country")
w_plan_by_signup_date = Window.partitionBy("plan").orderBy("signup_date", "id")
w_plan_by_signup_date_running = Window.partitionBy("plan").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_plan_by_birth_year_bucket_fu = Window.partitionBy("plan").orderBy("birth_year_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_by_signup_date_full = Window.partitionBy("plan").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_by_birth_year_bucket_ti = Window.partitionBy("plan").orderBy("birth_year_bucket")
w_signup_date_year_by_signup_d = Window.partitionBy(F.year("signup_date")).orderBy(F.col("signup_date").desc(), "id").rowsBetween(-1, -1)
w_signup_date_year_by_signup_d_2 = Window.partitionBy(F.year("signup_date")).orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_listeners
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_plan_by_birth_year_bucket), 3).alias("birth_year_bucket_percent_rank"),
        (F.col("birth_year") - F.lag("birth_year").over(w_plan_by_birth_year_bucket)).alias("birth_year_change"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_plan_country), F.lit(0)), 4).alias("birth_year_share_of_plan_country"),
        F.min("birth_year").over(w_plan_country).alias("plan_country_min_birth_year"),
        F.ntile(10).over(w_plan_by_signup_date).alias("signup_date_decile"),
        F.min("birth_year_bucket").over(w_plan_country).alias("plan_country_min_birth_year_bucket"),
        F.count(F.lit(1)).over(w_plan_country).alias("rows_in_plan_country"),
        F.last("birth_year_bucket", ignorenulls=True).over(w_plan_by_signup_date_running).alias("last_known_birth_year_bucket"),
        F.last("birth_year_bucket").over(w_plan_by_birth_year_bucket_fu).alias("last_birth_year_bucket"),
        F.nth_value("birth_year_bucket", 2).over(w_plan_by_signup_date_full).alias("second_birth_year_bucket"),
        F.round(F.col("birth_year_bucket") / F.nullif(F.sum("birth_year_bucket").over(w_plan_country), F.lit(0)), 4).alias("birth_year_bucket_share_of_plan_country"),
        F.max("birth_year").over(w_plan_country).alias("plan_country_max_birth_year"),
        F.dense_rank().over(w_plan_by_birth_year_bucket_ti).alias("birth_year_bucket_dense_rank"),
        F.round(F.avg("birth_year").over(w_signup_date_year_by_signup_d), 2).alias("birth_year_avg_1_prior"),
        F.nth_value("signup_date", 2).over(w_signup_date_year_by_signup_d_2).alias("second_signup_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        "birth_year_bucket",
        "birth_year_bucket_percent_rank",
        "birth_year_change",
        "birth_year_share_of_plan_country",
        "plan_country_min_birth_year",
        "signup_date_decile",
        "last_birth_year_bucket",
        "second_birth_year_bucket",
        "birth_year_bucket_share_of_plan_country",
        "birth_year_bucket_dense_rank",
        "birth_year_avg_1_prior",
        "second_signup_date",
        F.substring(F.trim(F.col("username")), 1, 3).alias("username_prefix"),
    )
    .orderBy(
        F.col("plan_country_min_birth_year").asc_nulls_last(),
        F.col("birth_year_change").desc(),
    )
)
