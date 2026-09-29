from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_title_len = Window.partitionBy("artist_id", "title_len")
w_all_by_is_explicit_flag_full = Window.orderBy(F.col("is_explicit_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_is_explicit_flag_prio = Window.orderBy(F.col("is_explicit_flag").desc(), "id").rowsBetween(-1, -1)
w_all_by_is_explicit_flag = Window.orderBy(F.col("is_explicit_flag").desc(), "id")

base_tracks = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-11-14").cast("date")) & (F.col("bpm") <= 127))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_activity = (
    base_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.size(F.col("t.moods")) >= 2), "left_anti")
)

screened = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.source").isin("playlist", "search")) & (F.col("p.device").isin("speaker", "car")), "left_semi")
    .filter(F.col("title_len") <= 12)
)

ranked = (
    screened
    .select(
        "*",
        F.round(F.avg("is_explicit_flag").over(w_artist_id_title_len), 2).alias("artist_id_title_len_avg_is_explicit_flag"),
        F.last("duration_sec").over(w_all_by_is_explicit_flag_full).alias("last_duration_sec"),
        F.round(F.avg("title_len").over(w_all_by_is_explicit_flag_prio), 2).alias("title_len_avg_1_prior"),
        F.first("release_date").over(w_all_by_is_explicit_flag_full).alias("first_release_date"),
        F.ntile(10).over(w_all_by_is_explicit_flag).alias("is_explicit_flag_decile"),
    )
)

result = (
    ranked
    .rollup("title_len", "is_explicit", "artist_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("title_len_avg_1_prior").alias("max_title_len_avg_1_prior"),
        F.min("is_explicit_flag_decile").alias("min_is_explicit_flag_decile"),
    )
    .orderBy(
        F.col("title_len").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
