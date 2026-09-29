from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country = Window.partitionBy("country")
w_all_by_signup_date_full = Window.orderBy(F.col("signup_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_signup_date_prior_6 = Window.orderBy(F.col("signup_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_latest = Window.partitionBy("country").orderBy(F.col("signup_date").desc(), F.col("id").desc(), F.col("play_id").desc())

base_listeners = (
    df_listeners
    .filter(F.col("signup_date") >= F.lit("2024-08-01").cast("date"))
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.when(F.col("birth_year") >= 1990, "HIGH").when(F.col("birth_year") >= 1970, "MEDIUM").otherwise("LOW").alias("birth_year_tier"),
        F.when(F.col("country").isin("BR", "JP"), "group_a").when(F.col("country") == "UK", "group_b").otherwise("other").alias("country_group"),
    )
)

windowed = (
    base_listeners
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "days_since_signup_date",
        "birth_year_tier",
        F.round(F.col("days_since_signup_date") / F.nullif(F.sum("days_since_signup_date").over(w_country), F.lit(0)), 4).alias("days_since_signup_date_share_of_country"),
        F.count(F.lit(1)).over(w_country).alias("rows_in_country"),
        F.nth_value("signup_date", 2).over(w_all_by_signup_date_full).alias("second_signup_date"),
        F.min("days_since_signup_date").over(w_country).alias("country_min_days_since_signup_date"),
        F.max("days_since_signup_date").over(w_all_by_signup_date_prior_6).alias("days_since_signup_date_max_6_prior"),
    )
)

with_children = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.username",
        "s.country",
        "s.signup_date",
        "s.days_since_signup_date",
        "s.birth_year_tier",
        "s.days_since_signup_date_share_of_country",
        "s.rows_in_country",
        "s.country_min_days_since_signup_date",
        "s.days_since_signup_date_max_6_prior",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

latest_per_key = (
    with_children
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "signup_date",
        "days_since_signup_date",
        "birth_year_tier",
        "days_since_signup_date_share_of_country",
        "rows_in_country",
        "country_min_days_since_signup_date",
        "play_id",
        "play_played_at",
        "play_is_skipped",
        "play_source",
        F.coalesce(F.col("play_source"), F.lit("UNKNOWN")).alias("play_source_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
    .orderBy(F.col("play_id").asc_nulls_last(), F.col("id").desc())
    .limit(10)
)
