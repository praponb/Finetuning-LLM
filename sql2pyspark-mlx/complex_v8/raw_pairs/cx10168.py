from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_fill = Window.partitionBy("is_skipped").orderBy("ms_played_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source = Window.partitionBy("source")
w_is_skipped_by_ms_played_fill_2 = Window.partitionBy("is_skipped").orderBy("ms_played_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_filled = Window.orderBy(F.col("ms_played_filled").desc(), "id")
w_rn_device = Window.partitionBy("rn", "device")
w_ms_played_filled_quintile_by = Window.partitionBy("ms_played_filled_quintile").orderBy(F.col("rn").desc(), "id").rowsBetween(-2, Window.currentRow)
w_track_id_by_distinct_listene = Window.partitionBy("track_id").orderBy(F.col("distinct_listener_id_in_rn_device").desc(), "id").rowsBetween(-3, Window.currentRow)
w_device_ms_played_filled_quin = Window.partitionBy("device", "ms_played_filled_quintile").orderBy(F.col("rn").desc(), "id")
w_is_skipped_distinct_listener = Window.partitionBy("is_skipped", "distinct_listener_id_in_rn_device").orderBy(F.col("second_ms_played_filled").desc(), "id")
w_ms_played_filled_quintile = Window.partitionBy("ms_played_filled_quintile")

cleaned_plays = (
    df_plays
    .filter(F.col("device") == "car")
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.when(F.col("ms_played") >= 246779, "A").when(F.col("ms_played") >= 145720, "B").otherwise("C").alias("ms_played_tier"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_parent = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

metrics = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "ms_played_filled",
        "track_title",
        "track_is_explicit",
        F.nth_value("ms_played_filled", 2).over(w_is_skipped_by_ms_played_fill).alias("second_ms_played_filled"),
        F.round(F.avg("ms_played_filled").over(w_source), 2).alias("source_avg_ms_played_filled"),
        F.first("track_duration_sec", ignorenulls=True).over(w_is_skipped_by_ms_played_fill_2).alias("first_known_track_duration_sec"),
        F.row_number().over(w_all_by_ms_played_filled).alias("rn"),
        F.ntile(5).over(w_all_by_ms_played_filled).alias("ms_played_filled_quintile"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_source), 2).alias("ms_played_filled_vs_source_avg"),
    )
)

ranked = (
    metrics
    .withColumn("distinct_listener_id_in_rn_device", F.size(F.collect_set("listener_id").over(w_rn_device)))
    .withColumn("rn_avg_3", F.round(F.avg("rn").over(w_ms_played_filled_quintile_by), 2))
)

sequenced = (
    ranked
    .select(
        "*",
        F.round(F.avg("ms_played_filled_quintile").over(w_track_id_by_distinct_listene), 2).alias("ms_played_filled_quintile_avg_4"),
        F.lead("ms_played_filled").over(w_device_ms_played_filled_quin).alias("next_ms_played_filled"),
        F.rank().over(w_is_skipped_distinct_listener).alias("second_ms_played_filled_rank"),
        F.count(F.lit(1)).over(w_ms_played_filled_quintile).alias("rows_in_ms_played_filled_quintile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "track_title",
        "second_ms_played_filled",
        "rn",
        "ms_played_filled_vs_source_avg",
        "rn_avg_3",
        "ms_played_filled_quintile_avg_4",
        "next_ms_played_filled",
        F.greatest(F.col("rn_avg_3"), F.col("rn")).alias("greatest_rn_avg_3_rn"),
    )
    .orderBy(
        "next_ms_played_filled",
        F.col("ms_played_filled_quintile_avg_4").desc(),
    )
)
