from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit_device = Window.partitionBy("track_is_explicit", "device")
w_device_by_played_at_month_ti = Window.partitionBy("device").orderBy("played_at_month")
w_source_track_artist_id_by_pl = Window.partitionBy("source", "track_artist_id").orderBy(F.col("played_at_month").desc())
w_device_by_played_at_month = Window.partitionBy("device").orderBy("played_at_month", "id")
w_source_track_artist_id_by_pl_2 = Window.partitionBy("source", "track_artist_id").orderBy(F.col("played_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_is_explicit_label = Window.partitionBy("track_is_explicit_label")
w_source_by_track_is_explicit = Window.partitionBy("source").orderBy(F.col("track_is_explicit_device_max_played_at_month").desc_nulls_first(), "id")
w_source_by_track_is_explicit_2 = Window.partitionBy("source").orderBy(F.col("track_is_explicit_device_max_played_at_month").desc_nulls_first())

base = (
    df_plays
    .filter((F.col("ms_played") > 73625) | (F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

enriched = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        "s.played_at_month",
        F.col("t.title").alias("track_title"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "played_at_month",
        "track_title",
        "track_is_explicit",
        "track_artist_id",
        F.min("played_at_month").over(w_track_is_explicit_device).alias("track_is_explicit_device_min_played_at_month"),
        F.round(F.cume_dist().over(w_device_by_played_at_month_ti), 3).alias("played_at_month_cume_dist"),
        F.round(F.col("played_at_month") / F.nullif(F.sum("played_at_month").over(w_track_is_explicit_device), F.lit(0)), 4).alias("played_at_month_share_of_track_is_explicit_devic"),
        F.round(F.percent_rank().over(w_source_track_artist_id_by_pl), 3).alias("played_at_month_percent_rank"),
        F.dense_rank().over(w_device_by_played_at_month).alias("played_at_month_dense_rank"),
        F.nth_value("played_at_month", 2).over(w_source_track_artist_id_by_pl_2).alias("second_played_at_month"),
        F.round(F.col("played_at_month") - F.avg("played_at_month").over(w_track_is_explicit_device), 2).alias("played_at_month_vs_track_is_explicit_device_avg"),
        F.max("played_at_month").over(w_track_is_explicit_device).alias("track_is_explicit_device_max_played_at_month"),
        F.count(F.lit(1)).over(w_track_is_explicit_device).alias("rows_in_track_is_explicit_device"),
    )
)

tagged = (
    with_trends
    .filter(F.col("device") == "car")
    .select(
        "*",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
        F.round(F.col("played_at_month") / F.nullif(F.col("second_played_at_month"), F.lit(0)), 3).alias("played_at_month_per_second_played_at_month"),
        F.when(F.col("track_is_explicit"), "yes").otherwise("no").alias("track_is_explicit_label"),
    )
)

windowed = (
    tagged
    .select(
        "id",
        "track_id",
        "source",
        "played_at_month",
        "track_title",
        "track_artist_id",
        "track_is_explicit_device_min_played_at_month",
        "played_at_month_cume_dist",
        "played_at_month_share_of_track_is_explicit_devic",
        "played_at_month_percent_rank",
        "played_at_month_dense_rank",
        "second_played_at_month",
        "played_at_month_vs_track_is_explicit_device_avg",
        "track_is_explicit_device_max_played_at_month",
        "device_clean",
        "track_is_explicit_label",
        F.round(F.avg("played_at_month_percent_rank").over(w_track_is_explicit_label), 2).alias("track_is_explicit_label_avg_played_at_month_perc"),
        F.lead("second_played_at_month").over(w_source_by_track_is_explicit).alias("next_second_played_at_month"),
        F.size(F.collect_set("track_artist_id").over(w_track_is_explicit_label)).alias("distinct_track_artist_id_in_track_is_explicit_la"),
        F.round(F.percent_rank().over(w_source_by_track_is_explicit_2), 3).alias("track_is_explicit_device_max_played_at_month_per"),
        F.lead("played_at_month_percent_rank").over(w_source_by_track_is_explicit).alias("next_played_at_month_percent_rank"),
    )
)

result = (
    windowed
    .select(
        "id",
        "track_id",
        "source",
        "played_at_month",
        "track_artist_id",
        "track_is_explicit_device_min_played_at_month",
        "played_at_month_cume_dist",
        "played_at_month_share_of_track_is_explicit_devic",
        "played_at_month_percent_rank",
        "played_at_month_dense_rank",
        "second_played_at_month",
        "played_at_month_vs_track_is_explicit_device_avg",
        "track_is_explicit_device_max_played_at_month",
        "device_clean",
        "track_is_explicit_label_avg_played_at_month_perc",
        "distinct_track_artist_id_in_track_is_explicit_la",
        "track_is_explicit_device_max_played_at_month_per",
        F.coalesce(F.col("second_played_at_month"), F.lit(0)).alias("second_played_at_month_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("device_clean")), " ", "_")).alias("device_clean_slug"),
    )
    .orderBy(
        F.col("track_is_explicit_device_max_played_at_month").desc(),
        "track_artist_id",
        "id",
    )
    .limit(10)
)
