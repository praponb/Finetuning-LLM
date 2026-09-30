from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_plan = Window.partitionBy("country", "plan")
w_first_played_at_month_signup = Window.partitionBy(F.month("first_played_at"), F.year("signup_date_year")).orderBy(F.col("last_played_at").desc())

filtered_listeners = (
    df_listeners
    .filter((~F.col("plan").isin("premium", "free")) & (F.col("birth_year") <= 1986))
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        F.substring(F.trim(F.col("country")), 1, 2).alias("country_prefix"),
        F.date_trunc("year", F.col("signup_date")).alias("signup_date_year"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_counts = (
    filtered_listeners.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.last_played_at")
)

windowed = (
    with_counts
    .select(
        "*",
        F.count(F.lit(1)).over(w_country_plan).alias("rows_in_country_plan"),
        F.rank().over(w_first_played_at_month_signup).alias("last_played_at_rank"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

annotated = (
    summarized_rows
    .filter(F.col("signup_date_year") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.coalesce(F.col("rows_in_country_plan"), F.lit(0)).alias("rows_in_country_plan_filled"),
        (F.col("first_played_at") + F.expr("INTERVAL 2 HOURS")).alias("first_played_at_plus_2h"),
        F.coalesce(F.col("days_since_signup_date"), F.lit(0)).alias("days_since_signup_date_filled"),
    )
)

result = (
    annotated
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "country_prefix",
        "signup_date_year",
        "first_played_at",
        "last_played_at_rank",
        "first_played_at_plus_2h",
        "days_since_signup_date_filled",
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
    )
    .orderBy(F.col("last_played_at_rank").desc_nulls_first(), "plan")
)
