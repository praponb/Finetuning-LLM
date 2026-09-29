from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_moods_count = Window.partitionBy("is_explicit", "moods_count")

recent = (
    df_tracks
    .filter((F.col("duration_sec") <= 357) | (F.col("bpm") < 170))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "release_date_label",
        "days_since_release_date",
        F.round(F.col("days_since_release_date") / F.nullif(F.sum("days_since_release_date").over(w_is_explicit_moods_count), F.lit(0)), 4).alias("days_since_release_date_share_of_is_explicit_moo"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_moods_count), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_moods_count"),
    )
)

rollup_base = (
    metrics
    .groupBy("artist_id")
    .agg(
        F.round(F.sum("days_since_release_date") / F.nullif(F.sum("duration_sec_share_of_is_explicit_moods_count"), F.lit(0)), 3).alias("days_since_release_date_to_duration_sec_share_of"),
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 6, 1).otherwise(0)).alias("n_length_trim_title_6"),
        F.max("days_since_release_date_share_of_is_explicit_moo").alias("max_days_since_release_date_share_of_is_explicit"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

labeled = (
    rollup_base
    .select(
        "artist_id",
        "days_since_release_date_to_duration_sec_share_of",
        "distinct_artist_ids",
        F.when(F.col("distinct_artist_ids") >= 8, "large").when(F.col("distinct_artist_ids") >= 3, "medium").otherwise("small").alias("distinct_artist_ids_tier"),
        F.abs(F.col("n_length_trim_title_6") - 6).alias("n_length_trim_title_6_distance"),
        F.round(F.col("max_duration_sec") / F.nullif(F.col("n_length_trim_title_6"), F.lit(0)), 3).alias("max_duration_sec_per_n_length_trim_title_6"),
    )
)

labeled_2 = (
    labeled
    .select(
        "artist_id",
        "days_since_release_date_to_duration_sec_share_of",
        "distinct_artist_ids",
        "distinct_artist_ids_tier",
        "max_duration_sec_per_n_length_trim_title_6",
        F.length(F.trim(F.col("distinct_artist_ids_tier"))).alias("distinct_artist_ids_tier_len"),
    )
)

with_activity = (
    labeled_2
    .crossJoin(labeled_2.agg(F.max("distinct_artist_ids_tier_len").alias("overall_max_distinct_artist_ids_tier_len")))
    .filter(F.col("distinct_artist_ids_tier_len") < F.col("overall_max_distinct_artist_ids_tier_len"))
    .drop("overall_max_distinct_artist_ids_tier_len")
)

result = (
    with_activity
    .groupBy("distinct_artist_ids_tier_len", "artist_id")
    .pivot("distinct_artist_ids_tier", ["large", "medium"])
    .agg(F.sum("max_duration_sec_per_n_length_trim_title_6").alias("amount"), F.count("max_duration_sec_per_n_length_trim_title_6").alias("n"))
)
