from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id", "source").orderBy(F.col("is_skipped_flag").desc(), F.col("id").desc())

prepared_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")) & (F.col("device").isin("tv", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

latest_per_key = (
    prepared_plays
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details_listener = (
    latest_per_key.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.source_ref",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    combined_details_listener
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "source_ref",
        "listener_signup_date",
        F.abs(F.col("ms_played") - 246779).alias("ms_played_distance"),
    )
    .orderBy("ms_played", F.col("id").desc())
)
