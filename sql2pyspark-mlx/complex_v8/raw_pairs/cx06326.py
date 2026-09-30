from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("bpm_minus_duration_sec").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_is_verified_is_explic_2 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy(F.col("bpm_minus_duration_sec").desc())
w_artist_id = Window.partitionBy("artist_id")
w_artist_genre_by_duration_sec = Window.partitionBy("artist_genre").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_rn = Window.partitionBy("artist_genre", "rn")
w_artist_genre_rn_by_distinct = Window.partitionBy("artist_genre", "rn").orderBy("distinct_artist_genre_in_is_explicit_artist_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_bpm_minus_duration_sec_qu = Window.partitionBy("rn", "bpm_minus_duration_sec_quintile").orderBy("min_duration_sec_bucket_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
    )
)

enriched = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.duration_sec_bucket",
        "s.bpm_minus_duration_sec",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_is_explicit_artist_id_by_dur)).alias("artist_debut_year_change"),
        F.ntile(5).over(w_artist_is_verified_is_explic).alias("bpm_minus_duration_sec_quintile"),
        F.size(F.collect_set("artist_genre").over(w_is_explicit_artist_id)).alias("distinct_artist_genre_in_is_explicit_artist_id"),
        F.rank().over(w_artist_is_verified_is_explic).alias("bpm_minus_duration_sec_rank"),
        F.round(F.percent_rank().over(w_artist_is_verified_is_explic_2), 3).alias("bpm_minus_duration_sec_percent_rank"),
        F.sum("bpm_minus_duration_sec").over(w_artist_id).alias("artist_id_sum_bpm_minus_duration_sec"),
        F.min("duration_sec_bucket").over(w_artist_genre_by_duration_sec).alias("min_duration_sec_bucket_so_far"),
        F.round(F.col("duration_sec_bucket") / F.nullif(F.sum("duration_sec_bucket").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("duration_sec_bucket_share_of_is_explicit_artist_"),
        F.row_number().over(w_artist_is_verified_is_explic).alias("rn"),
    )
)

leaders = sequenced.filter(F.col("bpm_minus_duration_sec_rank") <= 2)

metrics = (
    leaders
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_artist_genre_rn), 2).alias("artist_genre_rn_avg_duration_sec"),
        F.first("duration_sec_bucket", ignorenulls=True).over(w_artist_genre_rn_by_distinct).alias("first_known_duration_sec_bucket"),
        F.size(F.collect_set("artist_id").over(w_artist_genre_rn)).alias("distinct_artist_id_in_artist_genre_rn"),
        F.first("artist_debut_year", ignorenulls=True).over(w_rn_bpm_minus_duration_sec_qu).alias("first_known_artist_debut_year"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "duration_sec_bucket",
        "bpm_minus_duration_sec",
        "bpm_minus_duration_sec_quintile",
        "distinct_artist_genre_in_is_explicit_artist_id",
        "bpm_minus_duration_sec_rank",
        "bpm_minus_duration_sec_percent_rank",
        "artist_id_sum_bpm_minus_duration_sec",
        "duration_sec_bucket_share_of_is_explicit_artist_",
        "rn",
        "artist_genre_rn_avg_duration_sec",
        "first_known_duration_sec_bucket",
        (F.floor(F.col("bpm_minus_duration_sec") / 100) * 100).alias("bpm_minus_duration_sec_bucket"),
    )
    .orderBy(
        F.col("bpm_minus_duration_sec_quintile").desc_nulls_first(),
        F.col("rn").desc_nulls_first(),
    )
)
