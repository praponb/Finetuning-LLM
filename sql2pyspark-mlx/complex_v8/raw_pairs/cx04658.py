from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_artist_co = Window.partitionBy("artist_is_verified", "artist_country")
w_has_sad_by_bpm = Window.partitionBy("has_sad").orderBy("bpm", "id")
w_has_sad_by_bpm_ties = Window.partitionBy("has_sad").orderBy("bpm")
w_has_sad_by_bpm_running = Window.partitionBy("has_sad").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_has_sad_artist_is_verified_b = Window.partitionBy("has_sad", "artist_is_verified").orderBy("bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_has_sad_by_duration_sec_ties = Window.partitionBy("has_sad").orderBy("duration_sec")

cleaned = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

with_details_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.has_sad",
        "s.duration_sec_bucket",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "has_sad",
        "duration_sec_bucket",
        "artist_debut_year",
        "artist_is_verified",
        "artist_country",
        F.size(F.collect_set("artist_id").over(w_artist_is_verified_artist_co)).alias("distinct_artist_id_in_artist_is_verified_artist_"),
        (F.col("duration_sec_bucket") - F.lag("duration_sec_bucket").over(w_has_sad_by_bpm)).alias("duration_sec_bucket_change"),
        F.dense_rank().over(w_has_sad_by_bpm_ties).alias("bpm_dense_rank"),
        F.round(F.cume_dist().over(w_has_sad_by_bpm), 3).alias("bpm_cume_dist"),
        F.sum("duration_sec_bucket").over(w_has_sad_by_bpm_running).alias("running_duration_sec_bucket"),
        F.dense_rank().over(w_has_sad_artist_is_verified_b).alias("bpm_dense_rank_2"),
        F.max("artist_debut_year").over(w_artist_is_verified_artist_co).alias("artist_is_verified_artist_country_max_artist_deb"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.rank().over(w_has_sad_by_duration_sec_ties).alias("duration_sec_rank"),
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.count(F.lit(1)).over(w_artist_is_verified_artist_co).alias("rows_in_artist_is_verified_artist_country"),
    )
)

per_group = (
    sequenced
    .groupBy("artist_id", "has_sad", "artist_country")
    .agg(
        F.array_sort(F.collect_set("artist_country")).alias("artist_country_list"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.sum(F.when(~F.col("artist_is_verified"), F.col("artist_id_sum_duration_sec"))).alias("artist_id_sum_duration_sec_not_artist_is_verifie"),
        F.sum("bpm_dense_rank_2").alias("total_bpm_dense_rank_2"),
        F.count("*").alias("cnt"),
    )
)

result = (
    per_group
    .select(
        "artist_id",
        "has_sad",
        "artist_country_list",
        "title_list",
        "artist_id_sum_duration_sec_not_artist_is_verifie",
        "cnt",
        (F.floor(F.col("total_bpm_dense_rank_2") / 10) * 10).alias("total_bpm_dense_rank_2_bucket"),
        F.when(F.col("has_sad"), "yes").otherwise("no").alias("has_sad_label"),
    )
    .orderBy(F.col("cnt").asc())
)
