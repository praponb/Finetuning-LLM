from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_track_id = Window.partitionBy("source", "track_id")
w_all_by_track_duration_sec_ru = Window.orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_track_id_is_skipped_by_track = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("track_duration_sec").desc(), "id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_all_by_artist_debut_year_mov = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_track_duration_sec_ru_2 = Window.orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped_by_track_2 = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_track_duration_sec = Window.orderBy(F.col("track_duration_sec").desc(), "id")
w_device = Window.partitionBy("device")
w_device_source = Window.partitionBy("device", "source")

valid_plays = (
    df_plays
    .filter((F.col("ms_played") < 230798) & (F.col("listener_id") <= 17))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
    )
)

joined = (
    valid_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.round(F.avg("track_duration_sec").over(w_source_track_id), 2).alias("source_track_id_avg_track_duration_sec"),
        F.first("artist_debut_year", ignorenulls=True).over(w_all_by_track_duration_sec_ru).alias("first_known_artist_debut_year"),
        F.max("artist_debut_year").over(w_track_id).alias("track_id_max_artist_debut_year"),
        F.row_number().over(w_track_id_is_skipped_by_track).alias("rn"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_listener_id_device), 2).alias("artist_debut_year_vs_listener_id_device_avg"),
        F.lag("track_duration_sec").over(w_all_by_artist_debut_year).alias("prev_track_duration_sec"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_listener_id_device), 2).alias("track_duration_sec_vs_listener_id_device_avg"),
        F.round(F.avg("artist_debut_year").over(w_all_by_artist_debut_year_mov), 2).alias("artist_debut_year_avg_4"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_source_track_id), 2).alias("track_duration_sec_vs_source_track_id_avg"),
        F.first("track_duration_sec", ignorenulls=True).over(w_all_by_track_duration_sec_ru_2).alias("first_known_track_duration_sec"),
        F.count(F.lit(1)).over(w_source_track_id).alias("rows_in_source_track_id"),
        F.first("artist_debut_year").over(w_track_id_is_skipped_by_track_2).alias("first_artist_debut_year"),
        F.ntile(5).over(w_all_by_track_duration_sec).alias("track_duration_sec_quintile"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_listener_id_device), F.lit(0)), 4).alias("track_duration_sec_share_of_listener_id_device"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "source",
        "track_duration_sec",
        "artist_debut_year",
        "source_track_id_avg_track_duration_sec",
        "first_known_artist_debut_year",
        "track_id_max_artist_debut_year",
        "rn",
        "prev_track_duration_sec",
        "track_duration_sec_vs_listener_id_device_avg",
        "artist_debut_year_avg_4",
        "track_duration_sec_vs_source_track_id_avg",
        "first_known_track_duration_sec",
        "rows_in_source_track_id",
        "first_artist_debut_year",
        "track_duration_sec_share_of_listener_id_device",
        F.min("artist_debut_year").over(w_device).alias("device_min_artist_debut_year"),
        F.size(F.collect_set("source").over(w_device)).alias("distinct_source_in_device"),
        F.max("first_artist_debut_year").over(w_device_source).alias("device_source_max_first_artist_debut_year"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "source",
        "track_duration_sec",
        "source_track_id_avg_track_duration_sec",
        "first_known_artist_debut_year",
        "track_id_max_artist_debut_year",
        "track_duration_sec_vs_listener_id_device_avg",
        "artist_debut_year_avg_4",
        "track_duration_sec_vs_source_track_id_avg",
        "first_artist_debut_year",
        "device_min_artist_debut_year",
        "device_source_max_first_artist_debut_year",
        F.coalesce(F.col("track_duration_sec"), F.lit(0)).alias("track_duration_sec_filled"),
        F.coalesce(F.col("prev_track_duration_sec"), F.lit(0)).alias("prev_track_duration_sec_filled"),
    )
    .orderBy(
        F.col("track_duration_sec_vs_listener_id_device_avg").desc_nulls_first(),
        F.col("device_source_max_first_artist_debut_year").asc(),
    )
)
