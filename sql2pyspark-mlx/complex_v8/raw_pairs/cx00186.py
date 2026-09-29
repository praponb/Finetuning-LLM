from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_artist_genre_artist_country = Window.partitionBy("artist_genre", "artist_country").orderBy(F.col("bpm").desc())
w_artist_genre_artist_country_2 = Window.partitionBy("artist_genre", "artist_country").orderBy(F.col("bpm").desc(), "id")
w_artist_country_by_duration_s = Window.partitionBy("artist_country").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_country_by_duration_s_2 = Window.partitionBy("artist_country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_by_duration_s_3 = Window.partitionBy("artist_country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_country_by_duration_s_4 = Window.partitionBy("artist_country").orderBy("duration_sec", "id")
w_artist_country_by_duration_s_5 = Window.partitionBy("artist_country").orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_artist_genre_artist_country_3 = Window.partitionBy("artist_genre", "artist_country").orderBy(F.col("bpm").desc(), "id").rowsBetween(-3, -1)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_rank = Window.orderBy("bpm_rank", "id")
w_all_by_bpm_rank_running = Window.orderBy("bpm_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_rank_ties = Window.orderBy("bpm_rank")
w_duration_sec_seq_by_artist_g = Window.partitionBy("duration_sec_seq").orderBy("artist_genre_sum_bpm", "id")

base_tracks = (
    df_tracks
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_parent_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "HipHop"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

metrics = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "artist_name",
        "artist_country",
        F.size(F.collect_set("artist_id").over(w_artist_genre)).alias("distinct_artist_id_in_artist_genre"),
        F.round(F.cume_dist().over(w_artist_genre_artist_country), 3).alias("bpm_cume_dist"),
        F.sum("bpm").over(w_artist_genre).alias("artist_genre_sum_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_genre_artist_country_2)).alias("bpm_change"),
        F.sum("bpm").over(w_artist_country_by_duration_s).alias("bpm_sum_6_prior"),
        F.min("duration_sec").over(w_artist_country_by_duration_s_2).alias("min_duration_sec_so_far"),
        F.last("duration_sec").over(w_artist_country_by_duration_s_3).alias("last_duration_sec"),
        F.sum("duration_sec").over(w_artist_genre).alias("artist_genre_sum_duration_sec"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_genre), 2).alias("bpm_vs_artist_genre_avg"),
        F.rank().over(w_artist_genre_artist_country).alias("bpm_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_genre), F.lit(0)), 4).alias("duration_sec_share_of_artist_genre"),
        F.row_number().over(w_artist_country_by_duration_s_4).alias("duration_sec_seq"),
        F.max("duration_sec").over(w_artist_country_by_duration_s_5).alias("duration_sec_max_3_prior"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_genre), 2).alias("duration_sec_vs_artist_genre_avg"),
        F.round(F.avg("bpm").over(w_artist_genre_artist_country_3), 2).alias("bpm_avg_3_prior"),
    )
)

metrics_2 = (
    metrics
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "artist_name",
        "artist_country",
        "distinct_artist_id_in_artist_genre",
        "bpm_cume_dist",
        "artist_genre_sum_bpm",
        "bpm_change",
        "bpm_sum_6_prior",
        "min_duration_sec_so_far",
        "last_duration_sec",
        "artist_genre_sum_duration_sec",
        "bpm_vs_artist_genre_avg",
        "bpm_rank",
        "duration_sec_seq",
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.round(F.col("artist_genre_sum_bpm") - F.avg("artist_genre_sum_bpm").over(w_artist_id), 2).alias("artist_genre_sum_bpm_vs_artist_id_avg"),
        F.ntile(4).over(w_all_by_bpm_rank).alias("bpm_rank_quartile"),
        (F.col("bpm_vs_artist_genre_avg") - F.lag("bpm_vs_artist_genre_avg").over(w_all_by_bpm_rank)).alias("bpm_vs_artist_genre_avg_change"),
        F.first("bpm_avg_3_prior", ignorenulls=True).over(w_all_by_bpm_rank_running).alias("first_known_bpm_avg_3_prior"),
        F.round(F.cume_dist().over(w_all_by_bpm_rank), 3).alias("bpm_rank_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_bpm_rank_ties), 3).alias("bpm_rank_percent_rank"),
        (F.col("bpm_change") - F.lag("bpm_change").over(w_duration_sec_seq_by_artist_g)).alias("bpm_change_change"),
    )
)

result = (
    metrics_2
    .filter(F.col("bpm_vs_artist_genre_avg_change") <= 108.0)
    .select(
        "id",
        "artist_id",
        "bpm",
        "artist_name",
        "artist_country",
        "distinct_artist_id_in_artist_genre",
        "bpm_cume_dist",
        "artist_genre_sum_bpm",
        "bpm_change",
        "bpm_sum_6_prior",
        "last_duration_sec",
        "artist_genre_sum_duration_sec",
        "bpm_rank",
        "distinct_artist_country_in_artist_id",
        "bpm_vs_artist_genre_avg_change",
        "bpm_rank_cume_dist",
        "bpm_rank_percent_rank",
        "bpm_change_change",
    )
    .orderBy("artist_name", "id")
    .limit(20)
)
