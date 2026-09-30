from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_prior_6 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_artist_id_by_bpm_prior_1 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, -1)
w_dedup = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc(), F.col("id").desc())

prepared = (
    df_tracks
    .filter(F.col("duration_sec") < 353)
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_plus_duration_sec",
        "s.duration_sec_bucket",
        "s.bpm_bucket",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    joined_artist
    .select(
        "*",
        F.nth_value("duration_sec_bucket", 2).over(w_artist_id_by_bpm_full).alias("second_duration_sec_bucket"),
        F.round(F.avg("bpm").over(w_artist_id_by_bpm_prior_6), 2).alias("bpm_avg_6_prior"),
        F.round(F.avg("bpm_plus_duration_sec").over(w_artist_id_by_bpm_prior_1), 2).alias("bpm_plus_duration_sec_avg_1_prior"),
    )
)

latest_per_key = (
    with_trends
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    latest_per_key
    .select(
        "*",
        F.abs(F.col("duration_sec_bucket") - 206).alias("duration_sec_bucket_distance"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.greatest(F.col("duration_sec_bucket"), F.col("artist_debut_year")).alias("greatest_duration_sec_bucket_artist_debut_year"),
    )
)

result = (
    tagged
    .rollup("title_len", "is_explicit")
    .agg(
        F.percentile_approx("bpm_plus_duration_sec_avg_1_prior", 0.5).alias("median_bpm_plus_duration_sec_avg_1_prior"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)
