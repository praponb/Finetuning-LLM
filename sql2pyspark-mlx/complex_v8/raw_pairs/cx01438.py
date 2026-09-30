from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_title_len = Window.partitionBy("title_len")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_year_title_len = Window.partitionBy("release_date_year", "title_len").orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_release_date_year_title_len_2 = Window.partitionBy("release_date_year", "title_len").orderBy("bpm", "id")
w_next_title_len_by_duration_s = Window.partitionBy("next_title_len").orderBy("duration_sec_tercile")
w_next_title_len_by_duration_s_2 = Window.partitionBy("next_title_len").orderBy("duration_sec_tercile", "id")
w_release_date_year = Window.partitionBy("release_date_year")

cleaned = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("P%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.year(F.col("release_date")).alias("release_date_year"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count(F.lit(1)).over(w_all_by_duration_sec_running).alias("running_count"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_title_len), F.lit(0)), 4).alias("duration_sec_share_of_title_len"),
        F.round(F.col("title_len") - F.avg("title_len").over(w_title_len), 2).alias("title_len_vs_title_len_avg"),
        F.ntile(3).over(w_all_by_duration_sec).alias("duration_sec_tercile"),
        F.round(F.avg("duration_sec").over(w_release_date_year_title_len), 2).alias("duration_sec_avg_7"),
        F.lead("title_len", 1, 0).over(w_release_date_year_title_len_2).alias("next_title_len"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.cume_dist().over(w_next_title_len_by_duration_s), 3).alias("duration_sec_tercile_cume_dist"),
        F.ntile(4).over(w_next_title_len_by_duration_s_2).alias("duration_sec_tercile_quartile"),
        F.ntile(10).over(w_next_title_len_by_duration_s_2).alias("duration_sec_tercile_decile"),
        F.lag("duration_sec").over(w_next_title_len_by_duration_s_2).alias("prev_duration_sec"),
        F.round(F.col("next_title_len") / F.nullif(F.sum("next_title_len").over(w_release_date_year), F.lit(0)), 4).alias("next_title_len_share_of_release_date_year"),
    )
)

screened = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")), "left_semi")
)

artist_id_stats = (
    screened
    .groupBy("artist_id")
    .agg(F.percentile_approx("duration_sec_tercile_decile", 0.5).alias("median_duration_sec_tercile_decile"))
)

outliers = (
    screened.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.duration_sec_tercile_decile") >= F.col("g.median_duration_sec_tercile_decile"))
    .select(
        "s.*",
        "g.median_duration_sec_tercile_decile",
        F.round(F.col("s.duration_sec_tercile_decile") - F.col("g.median_duration_sec_tercile_decile"), 2).alias("duration_sec_tercile_decile_minus_median"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.count("*").alias("track_count"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

profiled = (
    outliers.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

with_parent = (
    profiled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "Pop")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

result = (
    with_parent
    .filter(F.col("duration_sec_tercile_quartile") <= 9)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "is_explicit_flag",
        "title_len",
        "running_count",
        "title_len_vs_title_len_avg",
        "duration_sec_tercile",
        "duration_sec_avg_7",
        "next_title_len",
        "duration_sec_tercile_cume_dist",
        "duration_sec_tercile_quartile",
        "duration_sec_tercile_decile",
        "next_title_len_share_of_release_date_year",
        "median_duration_sec_tercile_decile",
        "duration_sec_tercile_decile_minus_median",
        "track_is_explicit_count",
        F.substring(F.trim(F.col("artist_genre")), 1, 3).alias("artist_genre_prefix"),
    )
    .orderBy(
        F.col("next_title_len_share_of_release_date_year").asc(),
        "track_is_explicit_count",
    )
)
