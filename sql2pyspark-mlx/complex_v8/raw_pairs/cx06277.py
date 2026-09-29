from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id")
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_by_duration = Window.partitionBy("is_explicit_flag").orderBy("duration_sec", "id")
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified_artist_id_sum_is_explicit_fla", "is_explicit_flag").orderBy("artist_is_verified_artist_id_sum_is_explicit_fla", "id")
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit_flag").orderBy("artist_is_verified_artist_id_sum_is_explicit_fla", "id")
w_is_explicit_by_bpm_vs_artist = Window.partitionBy("is_explicit").orderBy(F.col("bpm_vs_artist_is_verified_artist_id_avg").desc())
w_artist_is_verified_artist_id_3 = Window.partitionBy("artist_is_verified_artist_id_sum_is_explicit_fla")
w_artist_is_verified_artist_id_4 = Window.partitionBy("artist_is_verified_artist_id_sum_is_explicit_fla", "is_explicit").orderBy(F.col("seq_no").desc(), "id")
w_artist_genre_seq_no_by_seq_n = Window.partitionBy("artist_genre", "seq_no").orderBy("seq_no")
w_artist_genre_seq_no_by_seq_n_2 = Window.partitionBy("artist_genre", "seq_no").orderBy("seq_no", "id")

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

with_parent_artist = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.is_explicit_flag",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

screened = (
    with_parent_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
)

with_trends = (
    screened
    .select(
        "*",
        F.min("is_explicit_flag").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_min_is_explicit_fla"),
        F.last("bpm").over(w_release_date_month_by_releas).alias("last_bpm"),
        F.sum("is_explicit_flag").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_sum_is_explicit_fla"),
        F.lead("duration_sec").over(w_is_explicit_flag_by_duration).alias("next_duration_sec"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_is_verified_artist_id), 2).alias("bpm_vs_artist_is_verified_artist_id_avg"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_artist_is_verified_artist_id_2).alias("seq_no"),
        F.dense_rank().over(w_artist_is_verified_is_explic).alias("artist_is_verified_artist_id_sum_is_explicit_fla_2"),
        F.rank().over(w_is_explicit_by_bpm_vs_artist).alias("bpm_vs_artist_is_verified_artist_id_avg_rank"),
        F.size(F.collect_set("artist_country").over(w_artist_is_verified_artist_id_3)).alias("distinct_artist_country_in_artist_is_verified_ar"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_is_verified_artist_id_2)).alias("bpm_change"),
    )
)

metrics = (
    ranked
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "artist_country",
        "artist_is_verified_artist_id_sum_is_explicit_fla",
        "bpm_vs_artist_is_verified_artist_id_avg",
        "seq_no",
        "bpm_vs_artist_is_verified_artist_id_avg_rank",
        "distinct_artist_country_in_artist_is_verified_ar",
        "bpm_change",
        (F.col("next_duration_sec") - F.lag("next_duration_sec").over(w_artist_is_verified_artist_id_4)).alias("next_duration_sec_change"),
        F.rank().over(w_artist_genre_seq_no_by_seq_n).alias("seq_no_rank"),
        F.row_number().over(w_artist_genre_seq_no_by_seq_n_2).alias("seq_no_seq"),
    )
)

result = (
    metrics
    .filter(F.col("title").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "artist_country",
        "artist_is_verified_artist_id_sum_is_explicit_fla",
        "bpm_vs_artist_is_verified_artist_id_avg",
        "seq_no",
        "bpm_vs_artist_is_verified_artist_id_avg_rank",
        "distinct_artist_country_in_artist_is_verified_ar",
        "bpm_change",
        "next_duration_sec_change",
        "seq_no_rank",
        "seq_no_seq",
        F.concat_ws("|", F.col("artist_country"), F.col("title")).alias("artist_country_title_label"),
        F.length(F.trim(F.col("artist_country"))).alias("artist_country_len"),
    )
    .orderBy("distinct_artist_country_in_artist_is_verified_ar", "seq_no_rank")
)
