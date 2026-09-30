from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source").orderBy(F.col("ms_played_distance").desc(), F.col("id").desc())

prepared = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.abs(F.col("ms_played") - 176302).alias("ms_played_distance"),
    )
)

unique_rows = (
    prepared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    unique_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.*",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

without_matches = (
    joined
    .crossJoin(joined.agg(F.avg("ms_played_distance").alias("overall_avg_ms_played_distance")))
    .filter(F.col("ms_played_distance") > F.col("overall_avg_ms_played_distance"))
    .drop("overall_avg_ms_played_distance")
)

result = (
    without_matches
    .groupingSets(
        [["source", "listener_id"], ["source"], ["listener_id"], []],
        "source", "listener_id",
    )
    .agg(
        F.sum(F.when(~F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_false"),
        F.min("track_release_date").alias("first_track_release_date"),
        F.sum(F.when(F.col("source") == "album", F.col("ms_played_distance")).otherwise(0)).alias("ms_played_distance_source_album"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "listener_id",
        "n_is_skipped_false",
        "first_track_release_date",
        "ms_played_distance_source_album",
    )
    .orderBy(F.col("source").asc_nulls_last(), F.col("listener_id").asc_nulls_last())
)
