from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_plan_source = Window.partitionBy("listener_plan", "source")
w_track_is_explicit_device_by = Window.partitionBy("track_is_explicit", "device").orderBy("ms_played", "id")
w_track_id = Window.partitionBy("track_id")
w_listener_country_device_by_l = Window.partitionBy("listener_country", "device").orderBy(F.col("listener_birth_year").desc(), "id")
w_listener_country_by_ms_playe = Window.partitionBy("listener_country").orderBy(F.col("ms_played_bucket").desc_nulls_first(), "id")
w_listener_country_device_by_l_2 = Window.partitionBy("listener_country", "device").orderBy(F.col("listener_birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_pct_listener_count = Window.partitionBy("device").orderBy(F.col("pct_listener_country_br").asc_nulls_last(), "device", "listener_country").rowsBetween(-6, Window.currentRow)
w_device = Window.partitionBy("device")

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_details_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.col("l.birth_year") > 1975)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_bucket",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

ranked = (
    with_details_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "listener_country",
        "listener_plan",
        "track_bpm",
        F.round(F.avg("ms_played_bucket").over(w_listener_plan_source), 2).alias("listener_plan_source_avg_ms_played_bucket"),
        F.lag("ms_played", 2).over(w_track_is_explicit_device_by).alias("prev2_ms_played"),
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.lag("ms_played_bucket", 2, 0).over(w_listener_country_device_by_l).alias("prev2_ms_played_bucket"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_track_id), 2).alias("listener_birth_year_vs_track_id_avg"),
        F.round(F.cume_dist().over(w_listener_country_by_ms_playe), 3).alias("ms_played_bucket_cume_dist"),
        F.round(F.avg("track_bpm").over(w_listener_plan_source), 2).alias("listener_plan_source_avg_track_bpm"),
        F.last("played_at", ignorenulls=True).over(w_listener_country_device_by_l_2).alias("last_known_played_at"),
    )
)

tagged = (
    ranked
    .filter(F.hour(F.col("last_known_played_at")).between(6, 10))
    .withColumn("listener_country_ref", F.concat(F.col("listener_country"), F.lit("#"), F.col("listener_id").cast("string")))
)

grouped = (
    tagged
    .filter(F.col("listener_birth_year_vs_track_id_avg").between(-1405.6, 803.2))
    .groupBy("device", "listener_country")
    .agg(
        F.round(F.sum("ms_played_bucket_cume_dist") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("ms_played_bucket_cume_dist_to_ms_played_ratio"),
        F.max("listener_plan_source_avg_track_bpm").alias("max_listener_plan_source_avg_track_bpm"),
        F.round(100 * F.count(F.when(F.col("listener_country") != "BR", 1)) / F.count("*"), 1).alias("pct_listener_country_br"),
    )
)

with_windows = (
    grouped
    .select(
        "device",
        "listener_country",
        "max_listener_plan_source_avg_track_bpm",
        "pct_listener_country_br",
        F.round(F.avg("pct_listener_country_br").over(w_device_by_pct_listener_count), 2).alias("pct_listener_country_br_avg_7"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.round(F.avg("pct_listener_country_br").over(w_device), 2).alias("device_avg_pct_listener_country_br"),
    )
)

result = (
    with_windows
    .select(
        "device",
        "listener_country",
        "max_listener_plan_source_avg_track_bpm",
        "pct_listener_country_br",
        "pct_listener_country_br_avg_7",
        "rows_in_device",
        "device_avg_pct_listener_country_br",
    )
    .orderBy(
        F.col("max_listener_plan_source_avg_track_bpm").asc(),
        F.col("device").asc_nulls_last(),
        "listener_country",
    )
    .limit(10)
)
