from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_year = Window.orderBy(F.col("release_date_year").asc_nulls_last(), "id", "play_id")
w_play_is_skipped_play_listene = Window.partitionBy("play_is_skipped", "play_listener_id")
w_play_device_2 = Window.partitionBy("play_device_2")
w_all_by_play_ms_played_filled = Window.orderBy(F.col("play_ms_played_filled").desc(), "id", "play_id", "play_id_2")
w_play_id_2_by_release_date_ye = Window.partitionBy("play_id_2").orderBy("release_date_year_change", "id", "play_id", "play_id_2").rowsBetween(-6, -1)
w_play_id_2_by_release_date_ye_2 = Window.partitionBy("play_id_2").orderBy("release_date_year_change", "id", "play_id", "play_id_2")

valid_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-10-23").cast("date"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.year(F.col("release_date")).alias("release_date_year"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

with_children_plays = (
    valid_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date_year",
        "s.release_date_label",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

joined = (
    with_children_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date_year",
        "s.release_date_label",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_device",
        "s.play_listener_id",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "title",
        "release_date_year",
        "play_id",
        "play_device",
        "play_listener_id",
        "artist_is_verified",
        (F.col("release_date_year") - F.lag("release_date_year").over(w_all_by_release_date_year)).alias("release_date_year_change"),
        F.size(F.collect_set("play_device").over(w_play_is_skipped_play_listene)).alias("distinct_play_device_in_play_is_skipped_play_lis"),
        F.max("release_date_year").over(w_play_is_skipped_play_listene).alias("play_is_skipped_play_listener_id_max_release_dat"),
        F.round(F.col("release_date_year") - F.avg("release_date_year").over(w_play_is_skipped_play_listene), 2).alias("release_date_year_vs_play_is_skipped_play_listen"),
    )
)

detail_rows_plays = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.play_id",
        "s.play_device",
        "s.play_listener_id",
        "s.release_date_year_change",
        "s.release_date_year_vs_play_is_skipped_play_listen",
        F.col("p.id").alias("play_id_2"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device_2"),
    )
)

derived = (
    detail_rows_plays
    .select(
        "*",
        F.coalesce(F.col("play_ms_played"), F.lit(0)).alias("play_ms_played_filled"),
        F.when(F.col("release_date_year_change") >= 405, "A").when(F.col("release_date_year_change") >= -810, "B").otherwise("C").alias("release_date_year_change_tier"),
        F.concat_ws("|", F.col("play_device"), F.col("play_device_2")).alias("play_device_play_device_2_label"),
    )
)

with_trends_2 = (
    derived
    .select(
        "id",
        "play_id",
        "play_listener_id",
        "release_date_year_change",
        "release_date_year_vs_play_is_skipped_play_listen",
        "play_id_2",
        "play_device_2",
        "play_ms_played_filled",
        "release_date_year_change_tier",
        "play_device_play_device_2_label",
        F.max("play_ms_played_filled").over(w_play_device_2).alias("play_device_2_max_play_ms_played_filled"),
        F.row_number().over(w_all_by_play_ms_played_filled).alias("rn"),
        F.sum("release_date_year_change").over(w_play_id_2_by_release_date_ye).alias("release_date_year_change_sum_6_prior"),
        F.dense_rank().over(w_play_id_2_by_release_date_ye_2).alias("release_date_year_change_dense_rank"),
    )
)

result = (
    with_trends_2
    .select(
        "id",
        "play_id",
        "release_date_year_change",
        "release_date_year_vs_play_is_skipped_play_listen",
        "play_id_2",
        "play_device_2",
        "play_ms_played_filled",
        "release_date_year_change_tier",
        "play_device_play_device_2_label",
        "play_device_2_max_play_ms_played_filled",
        "release_date_year_change_sum_6_prior",
        "release_date_year_change_dense_rank",
        F.lower(F.regexp_replace(F.trim(F.col("play_device_2")), " ", "_")).alias("play_device_2_slug"),
        F.length(F.trim(F.col("play_device_2"))).alias("play_device_2_len"),
    )
)
