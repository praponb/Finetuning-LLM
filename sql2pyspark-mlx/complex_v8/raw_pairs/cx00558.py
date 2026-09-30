from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_interest_pos_plan = Window.partitionBy("interest_pos", "plan")
w_country_or_unknown_by_days_s = Window.partitionBy("country_or_unknown").orderBy(F.col("days_since_signup_date").desc(), "id", "interest_pos")
w_interest_pos_signup_date_day = Window.partitionBy("interest_pos", "signup_date_dayofweek")
w_distinct_country_or_unknown = Window.partitionBy("distinct_country_or_unknown_in_interest_pos_plan").orderBy(F.col("distinct_country_or_unknown_in_interest_pos_plan").desc(), "id", "interest_pos")
w_interest_interest_pos_by_int = Window.partitionBy("interest", "interest_pos").orderBy("interest_pos_plan_min_days_since_signup_date", "id", "interest_pos").rowsBetween(-3, -1)
w_all_by_birth_year_running = Window.orderBy(F.col("birth_year").desc(), "id", "interest_pos").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_listeners
    .filter(F.upper(F.trim(F.col("username"))).like("H%"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
    )
)

per_label = (
    base
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "days_since_signup_date",
        "country_or_unknown",
        "signup_date_dayofweek",
        F.posexplode("interests").alias("interest_pos", "interest"),
    )
)

tagged = (
    per_label
    .filter(F.col("birth_year") < 1986)
    .withColumn("interest_or_unknown", F.coalesce(F.col("interest"), F.lit("UNKNOWN")))
)

with_trends = (
    tagged
    .select(
        "id",
        "birth_year",
        "country_or_unknown",
        "signup_date_dayofweek",
        "interest_pos",
        "interest",
        F.size(F.collect_set("country_or_unknown").over(w_interest_pos_plan)).alias("distinct_country_or_unknown_in_interest_pos_plan"),
        F.round(F.col("interest_pos") - F.avg("interest_pos").over(w_interest_pos_plan), 2).alias("interest_pos_vs_interest_pos_plan_avg"),
        F.min("days_since_signup_date").over(w_interest_pos_plan).alias("interest_pos_plan_min_days_since_signup_date"),
        F.lag("days_since_signup_date").over(w_country_or_unknown_by_days_s).alias("prev_days_since_signup_date"),
        F.max("days_since_signup_date").over(w_interest_pos_plan).alias("interest_pos_plan_max_days_since_signup_date"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.round(F.col("signup_date_dayofweek") / F.nullif(F.sum("signup_date_dayofweek").over(w_interest_pos_signup_date_day), F.lit(0)), 4).alias("signup_date_dayofweek_share_of_interest_pos_sign"),
        F.lead("birth_year", 1, 0).over(w_distinct_country_or_unknown).alias("next_birth_year"),
        F.max("signup_date_dayofweek").over(w_interest_interest_pos_by_int).alias("signup_date_dayofweek_max_3_prior"),
        F.round(F.col("interest_pos") - F.avg("interest_pos").over(w_interest_pos_signup_date_day), 2).alias("interest_pos_vs_interest_pos_signup_date_dayofwe"),
        F.sum("prev_days_since_signup_date").over(w_all_by_birth_year_running).alias("running_prev_days_since_signup_date"),
    )
)

result = (
    with_trends_2
    .filter(F.col("next_birth_year") <= 1986)
    .select(
        "id",
        "birth_year",
        "country_or_unknown",
        "signup_date_dayofweek",
        "interest",
        "distinct_country_or_unknown_in_interest_pos_plan",
        "interest_pos_vs_interest_pos_plan_avg",
        "prev_days_since_signup_date",
        "interest_pos_plan_max_days_since_signup_date",
        "signup_date_dayofweek_share_of_interest_pos_sign",
        "next_birth_year",
        "signup_date_dayofweek_max_3_prior",
        F.length(F.trim(F.col("interest"))).alias("interest_len"),
    )
)
