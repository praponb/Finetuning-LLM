from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_artist_id", "listener_id").orderBy("played_at_hour", "id")

base = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

labeled = (
    base
    .select(
        "*",
        F.upper(F.trim(F.col("played_at_label"))).alias("played_at_label_clean"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

annotated = (
    labeled
    .filter(F.col("played_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select(
        "*",
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

with_parent = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source_device_label",
        "s.played_at_label_clean",
        "s.played_at_date",
        "s.played_at_hour",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

unique_rows = (
    with_parent
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "track_id",
        "is_skipped",
        "played_at_date",
        "track_artist_id",
        F.upper(F.trim(F.col("played_at_label_clean"))).alias("played_at_label_clean_clean"),
        F.abs(F.col("played_at_hour") - 12).alias("played_at_hour_distance"),
    )
    .orderBy("track_id")
)
