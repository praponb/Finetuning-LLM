from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("source").orderBy(F.col("ms_played_filled").desc(), F.col("id").desc())
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy(F.col("ms_played").desc(), "id")
w_track_id_device = Window.partitionBy("track_id", "device")
w_distinct_ids = Window.partitionBy("distinct_ids")

valid = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")) & (F.col("ms_played") <= 333795))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    deduped
    .select(
        "*",
        F.lag("ms_played_filled", 2, 0).over(w_listener_id_device_by_ms_pla).alias("prev2_ms_played_filled"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_track_id_device), F.lit(0)), 4).alias("ms_played_filled_share_of_track_id_device"),
    )
)

per_group = (
    with_windows
    .groupBy("device", "source", "is_skipped")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("device").isin("speaker", "desktop", "phone"), 1)).alias("n_device_in_speaker_desktop_ph"),
        F.sum("ms_played").alias("sum_ms_played_check"),
    )
    .filter(F.col("sum_ms_played_check") > 107912)
    .drop("sum_ms_played_check")
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

annotated = (
    without_matches
    .filter(F.col("n_device_in_speaker_desktop_ph") < 9)
    .select(
        "*",
        F.abs(F.col("distinct_ids") - 5).alias("distinct_ids_distance"),
        F.when(F.col("source").isin("radio", "album"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
)

with_windows_2 = (
    annotated
    .select(
        "device",
        "source",
        "is_skipped",
        "n_device_in_speaker_desktop_ph",
        "distinct_ids_distance",
        F.count(F.lit(1)).over(w_distinct_ids).alias("rows_in_distinct_ids"),
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_distinct_ids), 2).alias("distinct_ids_vs_distinct_ids_avg"),
    )
)

result = (
    with_windows_2
    .select(
        "device",
        "is_skipped",
        "n_device_in_speaker_desktop_ph",
        "distinct_ids_distance",
        "rows_in_distinct_ids",
        "distinct_ids_vs_distinct_ids_avg",
        F.least(F.col("rows_in_distinct_ids"), F.col("distinct_ids_distance")).alias("least_rows_in_distinct_ids_distinct_ids_distance"),
    )
    .orderBy(
        F.col("distinct_ids_distance").asc_nulls_last(),
        "least_rows_in_distinct_ids_distinct_ids_distance",
    )
)
