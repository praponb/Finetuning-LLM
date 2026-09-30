from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_duration_sec = Window.orderBy(F.col("track_duration_sec").desc(), "id")
w_all_by_track_duration_sec_ti = Window.orderBy(F.col("track_duration_sec").desc())
w_played_at_month_device_by_ar = Window.partitionBy(F.month("played_at"), "device").orderBy(F.col("artist_debut_year").desc(), "id")
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_played_at_month = Window.partitionBy(F.month("played_at"))

filtered = (
    df_plays
    .filter((F.col("ms_played") >= 108979) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        (F.col("track_bpm") - F.lag("track_bpm").over(w_all_by_track_duration_sec)).alias("track_bpm_change"),
        F.dense_rank().over(w_all_by_track_duration_sec_ti).alias("track_duration_sec_dense_rank"),
        F.lag("played_at").over(w_played_at_month_device_by_ar).alias("prev_played_at"),
        F.count(F.lit(1)).over(w_listener_id_device).alias("rows_in_listener_id_device"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_month), 2).alias("ms_played_vs_played_at_month_avg"),
        F.round(F.avg("ms_played").over(w_listener_id_device), 2).alias("listener_id_device_avg_ms_played"),
        F.size(F.collect_set("track_id").over(w_listener_id_device)).alias("distinct_track_id_in_listener_id_device"),
    )
)

rollup_base = (
    sequenced
    .filter(F.col("device") == "car")
    .groupBy(
        "artist_country",
        "listener_id",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.min("prev_played_at").alias("first_prev_played_at"),
        F.round(F.avg("track_duration_sec_dense_rank"), 2).alias("avg_track_duration_sec_dense_rank"),
        F.sum("artist_debut_year").alias("sum_artist_debut_year_check"),
    )
    .filter(F.col("sum_artist_debut_year_check") > 1994)
    .drop("sum_artist_debut_year_check")
)

result = (
    rollup_base
    .select(
        "artist_country",
        "listener_id",
        "played_at_month",
        "device_list",
        F.concat(F.col("artist_country"), F.lit("#"), F.col("listener_id").cast("string")).alias("artist_country_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_country")), " ", "_")).alias("artist_country_slug"),
    )
    .orderBy(
        "artist_country",
        F.col("artist_country_ref").desc(),
        "listener_id",
        "played_at_month",
    )
    .limit(20)
)
