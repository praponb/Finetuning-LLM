from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ms_played_tier = Window.partitionBy("ms_played_tier")
w_source_by_ms_played_running = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_plays
    .filter((~F.col("device").isin("speaker", "desktop")) & (F.col("track_id") <= 26))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 261579, "large").when(F.col("ms_played") >= 73625, "medium").otherwise("small").alias("ms_played_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "ms_played_tier",
        "device_slug",
        F.count(F.lit(1)).over(w_ms_played_tier).alias("rows_in_ms_played_tier"),
        F.sum("ms_played").over(w_ms_played_tier).alias("ms_played_tier_sum_ms_played"),
        F.sum("ms_played").over(w_source_by_ms_played_running).alias("running_ms_played"),
    )
)

joined_track = (
    sequenced.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.rows_in_ms_played_tier",
        "s.running_ms_played",
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    joined_track
    .groupingSets(
        [["is_skipped", "listener_id", "track_id"], ["is_skipped"], []],
        "is_skipped", "listener_id", "track_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("rows_in_ms_played_tier") / F.nullif(F.sum("running_ms_played"), F.lit(0)), 3).alias("rows_in_ms_played_tier_to_running_ms_played_rati"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
