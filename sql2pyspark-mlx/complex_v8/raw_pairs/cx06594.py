from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("plan", "play_device").orderBy("play_played_at_date", "id", "play_id")
w_dedup = Window.partitionBy("country", "play_track_id").orderBy("play_played_at", "id", "play_id")

scoped_listeners = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-10-17").cast("date")) | (F.upper(F.trim(F.col("username"))).like("H%")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        F.concat(F.col("country"), F.lit("#"), F.col("birth_year").cast("string")).alias("country_ref"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

annotated = (
    scoped_listeners
    .select(
        "*",
        F.round(F.col("birth_year") / F.nullif(F.col("days_since_signup_date"), F.lit(0)), 3).alias("birth_year_per_days_since_signup_date"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.plan",
        "s.country",
        "s.signup_date",
        "s.country_ref",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

enriched = (
    exploded_detail
    .filter(F.col("country").isin("BR", "UK", "KR"))
    .select(
        "id",
        "plan",
        "country",
        "play_id",
        "play_track_id",
        "play_played_at",
        "play_device",
        F.month(F.col("signup_date")).alias("signup_date_month"),
        F.to_date(F.col("play_played_at")).alias("play_played_at_date"),
    )
)

unique_rows = (
    enriched
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

filtered = (
    unique_rows
    .crossJoin(unique_rows.agg(F.avg("signup_date_month").alias("overall_avg_signup_date_month")))
    .filter(F.col("signup_date_month") > F.col("overall_avg_signup_date_month"))
    .drop("overall_avg_signup_date_month")
)

unique_rows_2 = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows_2
    .select(
        "id",
        "plan",
        "country",
        "play_id",
        "play_track_id",
        "play_device",
        "signup_date_month",
    )
    .orderBy(
        F.col("play_track_id").asc_nulls_last(),
        "country",
        "id",
        "play_id",
    )
    .limit(5)
)
