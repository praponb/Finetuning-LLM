from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len = Window.partitionBy("title_len")
w_title_len_by_title_len_full = Window.partitionBy("title_len").orderBy("title_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_title_len_artist_id_by_durat = Window.partitionBy("title_len", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last())
w_artist_id_play_id_by_title_l = Window.partitionBy("artist_id", "play_id").orderBy(F.col("title_len").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_len_by_title_len_runni = Window.partitionBy("title_len").orderBy("title_len", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_play_id_by_title_l_2 = Window.partitionBy("artist_id", "play_id").orderBy(F.col("title_len").desc(), "id", "play_id").rowsBetween(-6, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter((F.col("bpm") < 143) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

metrics = (
    recent_tracks
    .select(
        "*",
        F.sum("duration_sec").over(w_title_len).alias("title_len_sum_duration_sec"),
        F.first("bpm").over(w_title_len_by_title_len_full).alias("first_bpm"),
        F.min("duration_sec").over(w_title_len).alias("title_len_min_duration_sec"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_year), 2).alias("bpm_avg_6_prior"),
        F.round(F.cume_dist().over(w_title_len_artist_id_by_durat), 3).alias("duration_sec_cume_dist"),
    )
)

with_children = (
    metrics.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.title_len",
        "s.title_len_sum_duration_sec",
        "s.title_len_min_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_trends = (
    with_children
    .select(
        "*",
        F.sum("play_ms_played").over(w_artist_id_play_id_by_title_l).alias("running_play_ms_played"),
        F.first("release_date", ignorenulls=True).over(w_title_len_by_title_len_runni).alias("first_known_release_date"),
        F.round(F.avg("title_len").over(w_artist_id_play_id_by_title_l_2), 2).alias("title_len_avg_7"),
    )
)

result = (
    with_trends
    .groupBy("release_date", "artist_id")
    .pivot("play_source", ["album", "playlist", "radio"])
    .agg(F.sum("title_len"))
    .orderBy("release_date", "artist_id")
)
