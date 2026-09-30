from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("is_explicit_label", "artist_id").orderBy(F.col("release_date_label_len").desc(), F.col("id").desc())

prepared_tracks = (
    df_tracks
    .filter(F.col("bpm") <= 142)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

qualified = (
    prepared_tracks
    .crossJoin(prepared_tracks.agg(F.max("is_explicit_flag").alias("overall_max_is_explicit_flag")))
    .filter(F.col("is_explicit_flag") < F.col("overall_max_is_explicit_flag"))
    .drop("overall_max_is_explicit_flag")
)

scored = qualified.withColumn("release_date_label_len", F.length(F.trim(F.col("release_date_label"))))

deduped = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .rollup("release_date_label_len", "is_explicit_label", "artist_id")
    .agg(
        F.round(F.sum("bpm") / F.nullif(F.sum("is_explicit_flag"), F.lit(0)), 3).alias("bpm_to_is_explicit_flag_ratio"),
        F.count(F.when(F.lower(F.col("title")).like("%ng%"), 1)).alias("n_lower_title_like_ng"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
    )
)
