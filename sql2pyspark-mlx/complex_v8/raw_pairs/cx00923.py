from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_track_release = Window.partitionBy("listener_id").orderBy("track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_release_date = Window.orderBy(F.col("track_release_date").desc(), "id")
w_listener_id_by_track_release_2 = Window.partitionBy("listener_id").orderBy("track_release_date", "id")
w_device = Window.partitionBy("device")
w_is_skipped_track_id_by_ms_pl = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played").desc())
w_track_release_date_month = Window.partitionBy(F.date_trunc("month", "track_release_date"))
w_is_skipped_track_id_by_ms_pl_2 = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played").desc(), "id")

cleaned_plays = (
    df_plays
    .filter(F.col("ms_played") >= 56858)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
    )
)

enriched = (
    cleaned_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.is_explicit")), "left")
    .select("s.*", F.col("t.release_date").alias("track_release_date"))
)

metrics = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_listener_id_by_track_release).alias("running_count"),
        F.dense_rank().over(w_all_by_track_release_date).alias("track_release_date_dense_rank"),
        F.lag("track_release_date").over(w_listener_id_by_track_release_2).alias("prev_track_release_date"),
        F.size(F.collect_set("listener_id").over(w_device)).alias("distinct_listener_id_in_device"),
        F.round(F.percent_rank().over(w_all_by_track_release_date), 3).alias("track_release_date_percent_rank"),
        F.rank().over(w_is_skipped_track_id_by_ms_pl).alias("ms_played_rank"),
        F.round(F.cume_dist().over(w_all_by_track_release_date), 3).alias("track_release_date_cume_dist"),
        F.max("ms_played").over(w_track_release_date_month).alias("track_release_date_month_max_ms_played"),
        F.round(F.avg("ms_played").over(w_track_release_date_month), 2).alias("track_release_date_month_avg_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device), F.lit(0)), 4).alias("ms_played_share_of_device"),
        F.round(F.percent_rank().over(w_is_skipped_track_id_by_ms_pl_2), 3).alias("ms_played_percent_rank"),
        F.ntile(3).over(w_is_skipped_track_id_by_ms_pl_2).alias("ms_played_tercile"),
    )
)

leaders = (
    metrics
    .filter((F.col("track_release_date_dense_rank") <= 5) & (F.col("prev_track_release_date") < F.lit("2025-04-02").cast("date")))
)

result = (
    leaders
    .filter(F.col("ms_played_share_of_device").between(0.2, 0.7))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "track_release_date",
        "prev_track_release_date",
        "distinct_listener_id_in_device",
        "ms_played_rank",
        "track_release_date_cume_dist",
        "track_release_date_month_avg_ms_played",
        "ms_played_percent_rank",
        "ms_played_tercile",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.round(F.col("running_count") / F.nullif(F.col("ms_played"), F.lit(0)), 3).alias("running_count_per_ms_played"),
    )
)
