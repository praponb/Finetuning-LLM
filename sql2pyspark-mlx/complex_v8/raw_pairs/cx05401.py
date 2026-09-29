from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len_is_explicit = Window.partitionBy("title_len", "is_explicit")
w_artist_id_title_len_by_bpm_p = Window.partitionBy("artist_id", "title_len").orderBy("bpm", "id").rowsBetween(-1, -1)
w_artist_id_title_len_by_bpm_t = Window.partitionBy("artist_id", "title_len").orderBy("bpm")
w_artist_id_title_len_by_bpm = Window.partitionBy("artist_id", "title_len").orderBy("bpm", "id")

scoped_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("N%")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

windowed = (
    scoped_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "release_date_week",
        "title_len",
        F.round(F.col("title_len") - F.avg("title_len").over(w_title_len_is_explicit), 2).alias("title_len_vs_title_len_is_explicit_avg"),
        F.max("title_len").over(w_artist_id_title_len_by_bpm_p).alias("title_len_max_1_prior"),
        F.size(F.collect_set("artist_id").over(w_title_len_is_explicit)).alias("distinct_artist_id_in_title_len_is_explicit"),
        F.min("title_len").over(w_title_len_is_explicit).alias("title_len_is_explicit_min_title_len"),
        F.round(F.percent_rank().over(w_artist_id_title_len_by_bpm_t), 3).alias("bpm_percent_rank"),
        F.round((F.unix_timestamp("release_date_week") - F.unix_timestamp(F.lag("release_date_week").over(w_artist_id_title_len_by_bpm))) / 60, 1).alias("minutes_since_prev_release_date_week"),
    )
)

joined = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

result = (
    joined
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "title_len_vs_title_len_is_explicit_avg",
        "title_len_max_1_prior",
        "distinct_artist_id_in_title_len_is_explicit",
        "title_len_is_explicit_min_title_len",
        "minutes_since_prev_release_date_week",
        "artist_is_verified",
        F.when(F.col("title_len_max_1_prior") >= 26, "A").when(F.col("title_len_max_1_prior") >= 18, "B").otherwise("C").alias("title_len_max_1_prior_tier"),
    )
    .orderBy("release_date", "id")
)
