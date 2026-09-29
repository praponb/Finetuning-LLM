from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year = Window.orderBy("birth_year", "id")
w_plan_by_signup_date = Window.partitionBy("plan").orderBy(F.col("signup_date").desc(), "id")
w_plan_by_signup_date_ties = Window.partitionBy("plan").orderBy(F.col("signup_date").desc())
w_plan_country = Window.partitionBy("plan", "country")
w_all_by_birth_year_2 = Window.orderBy(F.col("birth_year").desc(), "id")
w_plan_by_signup_date_prior_1 = Window.partitionBy("plan").orderBy(F.col("signup_date").desc(), "id").rowsBetween(-1, -1)
w_plan_by_signup_date_2 = Window.partitionBy("plan").orderBy("signup_date", "id")
w_plan_by_signup_date_full = Window.partitionBy("plan").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_signup_date_year = Window.partitionBy(F.year("signup_date"))
w_all_by_birth_year_ties = Window.orderBy("birth_year")
w_plan_by_signup_date_running = Window.partitionBy("plan").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_decile_by_first = Window.partitionBy("signup_date_decile").orderBy("first_known_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_quartile = Window.partitionBy("signup_date_quartile")
w_signup_date_decile_signup_da = Window.partitionBy("signup_date_decile", F.month("signup_date")).orderBy(F.col("birth_year_percent_rank").desc(), "id")
w_all_by_birth_year_prior_6 = Window.orderBy(F.col("birth_year").desc_nulls_first(), "id").rowsBetween(-6, -1)

prepared_listeners = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 1) & (F.col("birth_year") < 1986))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM").alias("signup_date_label"),
    )
)

metrics = (
    prepared_listeners
    .select(
        "*",
        F.lead("birth_year", 1, 0).over(w_all_by_birth_year).alias("next_birth_year"),
        F.ntile(4).over(w_plan_by_signup_date).alias("signup_date_quartile"),
        F.round(F.percent_rank().over(w_plan_by_signup_date_ties), 3).alias("signup_date_percent_rank"),
        F.max("birth_year").over(w_plan_country).alias("plan_country_max_birth_year"),
        (F.col("birth_year") - F.lag("birth_year").over(w_all_by_birth_year_2)).alias("birth_year_change"),
        F.sum("birth_year").over(w_plan_by_signup_date_prior_1).alias("birth_year_sum_1_prior"),
        F.lag("birth_year").over(w_plan_by_signup_date).alias("prev_birth_year"),
        F.ntile(10).over(w_plan_by_signup_date_2).alias("signup_date_decile"),
        F.lead("birth_year", 2).over(w_plan_by_signup_date_2).alias("next2_birth_year"),
        F.last("birth_year").over(w_plan_by_signup_date_full).alias("last_birth_year"),
        F.round(F.col("birth_year") / F.nullif(F.sum("birth_year").over(w_signup_date_year), F.lit(0)), 4).alias("birth_year_share_of_signup_date_year"),
        F.count(F.lit(1)).over(w_signup_date_year).alias("rows_in_signup_date_year"),
        F.round(F.percent_rank().over(w_all_by_birth_year_ties), 3).alias("birth_year_percent_rank"),
        (F.col("birth_year") - F.lag("birth_year").over(w_plan_by_signup_date)).alias("birth_year_change_2"),
        F.first("birth_year", ignorenulls=True).over(w_plan_by_signup_date_running).alias("first_known_birth_year"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.max("signup_date_percent_rank").over(w_signup_date_decile_by_first).alias("max_signup_date_percent_rank_so_far"),
        F.round(F.avg("signup_date_quartile").over(w_signup_date_quartile), 2).alias("signup_date_quartile_avg_signup_date_quartile"),
        F.lead("birth_year_change").over(w_signup_date_decile_signup_da).alias("next_birth_year_change"),
        F.round(F.avg("birth_year_change").over(w_all_by_birth_year_prior_6), 2).alias("birth_year_change_avg_6_prior"),
        F.min("first_known_birth_year").over(w_signup_date_quartile).alias("signup_date_quartile_min_first_known_birth_year"),
        F.round(F.col("birth_year_sum_1_prior") / F.nullif(F.sum("birth_year_sum_1_prior").over(w_signup_date_quartile), F.lit(0)), 4).alias("birth_year_sum_1_prior_share_of_signup_date_quar"),
    )
)

result = (
    ranked
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        "signup_date_label",
        "next_birth_year",
        "signup_date_quartile",
        "signup_date_percent_rank",
        "plan_country_max_birth_year",
        "signup_date_decile",
        "next2_birth_year",
        "birth_year_share_of_signup_date_year",
        "rows_in_signup_date_year",
        "birth_year_percent_rank",
        "birth_year_change_2",
        "first_known_birth_year",
        "max_signup_date_percent_rank_so_far",
        "next_birth_year_change",
        "birth_year_change_avg_6_prior",
        "signup_date_quartile_min_first_known_birth_year",
        "birth_year_sum_1_prior_share_of_signup_date_quar",
    )
    .orderBy("signup_date_quartile_min_first_known_birth_year")
)
