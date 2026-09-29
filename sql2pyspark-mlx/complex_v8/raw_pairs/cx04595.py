from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_hour = Window.partitionBy("played_at_hour")

filtered = (
    df_plays
    .filter(F.col("ms_played") >= 145720)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag_2"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped_flag",
        "played_at_date",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

filtered_2 = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.ms_played") < 223828), "left_semi")
)

derived = (
    filtered_2
    .select(
        "*",
        (F.col("played_at_hour") + F.col("is_skipped_flag")).alias("played_at_hour_plus_is_skipped_flag"),
    )
)

annotated = derived.withColumn("played_at_hour_filled", F.coalesce(F.col("played_at_hour"), F.lit(0)))

ranked = (
    annotated
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped_flag",
        "played_at_year",
        "played_at_hour",
        "played_at_hour_plus_is_skipped_flag",
        "played_at_hour_filled",
        F.round(F.avg("is_skipped_flag").over(w_played_at_hour), 2).alias("played_at_hour_avg_is_skipped_flag"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_hour), 2).alias("ms_played_vs_played_at_hour_avg"),
    )
)

result = (
    ranked
    .filter(F.col("played_at_year") >= F.lit("2025-01-17 09:00:00").cast("timestamp"))
    .select(
        "id",
        "track_id",
        "is_skipped_flag",
        "played_at_year",
        "played_at_hour",
        "played_at_hour_filled",
        "ms_played_vs_played_at_hour_avg",
        F.date_trunc("week", F.col("played_at_year")).alias("played_at_year_week"),
    )
    .orderBy(F.col("played_at_hour").asc_nulls_last())
)
