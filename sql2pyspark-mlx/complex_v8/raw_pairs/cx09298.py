from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_artist_id").orderBy(F.col("track_duration_sec").desc(), F.col("track_id").desc(), F.col("is_skipped").desc(), F.col("ms_played_filled_tier").desc())
w_last_played_at_month = Window.partitionBy(F.month("last_played_at"))
w_is_skipped_by_first_played_a = Window.partitionBy("is_skipped").orderBy(F.col("first_played_at").desc())
w_ms_played_filled_tier_by_ms = Window.partitionBy("ms_played_filled_tier").orderBy(F.col("ms_played_filled_plus_ms_played_device_phone").desc(), "track_id", "is_skipped", "ms_played_filled_tier").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_first_played_a_2 = Window.partitionBy("is_skipped").orderBy(F.col("first_played_at").desc(), "track_id", "is_skipped", "ms_played_filled_tier")
w_ms_played_filled_tier_by_ms_2 = Window.partitionBy("ms_played_filled_tier").orderBy(F.col("ms_played_filled_plus_ms_played_device_phone").desc(), "track_id", "is_skipped", "ms_played_filled_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("source") != "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("source").isin("album", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
)

annotated = (
    recent_plays
    .select(
        "*",
        F.when(F.col("ms_played_filled") >= 281500, "HIGH").when(F.col("ms_played_filled") >= 103750, "MEDIUM").otherwise("LOW").alias("ms_played_filled_tier"),
        (F.col("ms_played_filled") + F.col("ms_played")).alias("ms_played_filled_plus_ms_played"),
    )
)

grouped = (
    annotated
    .groupBy("track_id", "is_skipped", "ms_played_filled_tier")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.countDistinct("ms_played_filled_tier").alias("distinct_ms_played_filled_tiers"),
        F.sum(F.when(F.col("device") == "phone", F.col("ms_played_filled_plus_ms_played")).otherwise(0)).alias("ms_played_filled_plus_ms_played_device_phone"),
        F.max("played_at").alias("last_played_at"),
        F.avg("ms_played_filled_plus_ms_played").alias("avg_ms_played_filled_plus_ms_played_check"),
    )
    .filter(F.col("avg_ms_played_filled_plus_ms_played_check") > 124250)
    .drop("avg_ms_played_filled_plus_ms_played_check")
)

enriched_track = (
    grouped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.track_id",
        "s.is_skipped",
        "s.ms_played_filled_tier",
        "s.first_played_at",
        "s.ms_played_filled_plus_ms_played_device_phone",
        "s.last_played_at",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

latest_per_key = (
    enriched_track
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest_per_key
    .select(
        "*",
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_last_played_at_month), F.lit(0)), 4).alias("track_duration_sec_share_of_last_played_at_month"),
        F.rank().over(w_is_skipped_by_first_played_a).alias("first_played_at_rank"),
        F.first("track_duration_sec").over(w_ms_played_filled_tier_by_ms).alias("first_track_duration_sec"),
        F.ntile(5).over(w_is_skipped_by_first_played_a_2).alias("first_played_at_quintile"),
        F.last("last_played_at", ignorenulls=True).over(w_ms_played_filled_tier_by_ms_2).alias("last_known_last_played_at"),
    )
)

screened = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.track_artist_id")) & (F.col("t.duration_sec") < 312), "left_anti")
    .filter(F.col("first_track_duration_sec") >= 209)
)

result = (
    screened
    .select(
        "track_id",
        "is_skipped",
        "first_played_at",
        "last_played_at",
        "track_duration_sec",
        "track_artist_id",
        "track_bpm",
        "track_duration_sec_share_of_last_played_at_month",
        "first_played_at_rank",
        "first_track_duration_sec",
        "last_known_last_played_at",
    )
    .orderBy(
        F.col("first_played_at_rank").desc_nulls_first(),
        F.col("track_artist_id").desc(),
    )
)
