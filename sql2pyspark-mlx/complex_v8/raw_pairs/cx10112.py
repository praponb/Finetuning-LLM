from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_artist_ids = Window.partitionBy("distinct_artist_ids")
w_n_is_explicit_true_n_is_expl = Window.partitionBy("n_is_explicit_true", "n_is_explicit_true_distance").orderBy(F.col("n_is_explicit_true_plus_total_duration_sec").desc(), "is_explicit", "duration_sec_tier")
w_n_is_explicit_true_by_distin = Window.partitionBy("n_is_explicit_true").orderBy(F.col("distinct_artist_ids").desc(), "is_explicit", "duration_sec_tier").rowsBetween(-2, Window.currentRow)

valid = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-05-12").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 127, "HIGH").when(F.col("bpm") >= 100, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

summary = (
    valid
    .groupBy(
        "is_explicit",
        F.when(F.col("duration_sec") >= 392, "large").when(F.col("duration_sec") >= 183, "medium").otherwise("small").alias("duration_sec_tier"),
    )
    .agg(
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(F.sum("duration_sec") / F.nullif(F.sum("bpm"), F.lit(0)), 3).alias("duration_sec_to_bpm_ratio"),
    )
)

labeled = (
    summary
    .select(
        "*",
        F.abs(F.col("n_is_explicit_true") - 4).alias("n_is_explicit_true_distance"),
        F.concat(F.col("duration_sec_tier"), F.lit("#"), F.col("distinct_artist_ids").cast("string")).alias("duration_sec_tier_ref"),
        (F.col("n_is_explicit_true") + F.col("total_duration_sec")).alias("n_is_explicit_true_plus_total_duration_sec"),
    )
)

tagged = (
    labeled
    .select(
        "is_explicit",
        "duration_sec_tier",
        "release_date_span_days",
        "n_is_explicit_true",
        "distinct_artist_ids",
        "duration_sec_to_bpm_ratio",
        "n_is_explicit_true_distance",
        "n_is_explicit_true_plus_total_duration_sec",
        F.least(F.col("total_duration_sec"), F.col("n_is_explicit_true")).alias("least_total_duration_sec_n_is_explicit_true"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.round(F.avg("n_is_explicit_true_distance").over(w_distinct_artist_ids), 2).alias("distinct_artist_ids_avg_n_is_explicit_true_dista"),
        F.round(F.col("distinct_artist_ids") / F.nullif(F.sum("distinct_artist_ids").over(w_distinct_artist_ids), F.lit(0)), 4).alias("distinct_artist_ids_share_of_distinct_artist_ids"),
        F.lag("n_is_explicit_true", 2).over(w_n_is_explicit_true_n_is_expl).alias("prev2_n_is_explicit_true"),
        F.ntile(10).over(w_n_is_explicit_true_n_is_expl).alias("n_is_explicit_true_plus_total_duration_sec_decil"),
        F.round(F.avg("release_date_span_days").over(w_n_is_explicit_true_by_distin), 2).alias("release_date_span_days_avg_3"),
    )
)

tagged_2 = (
    with_windows
    .select(
        "is_explicit",
        "duration_sec_tier",
        "n_is_explicit_true",
        "distinct_artist_ids",
        "n_is_explicit_true_distance",
        "n_is_explicit_true_plus_total_duration_sec",
        "least_total_duration_sec_n_is_explicit_true",
        "distinct_artist_ids_avg_n_is_explicit_true_dista",
        "distinct_artist_ids_share_of_distinct_artist_ids",
        "prev2_n_is_explicit_true",
        "release_date_span_days_avg_3",
        F.lower(F.regexp_replace(F.trim(F.col("duration_sec_tier")), " ", "_")).alias("duration_sec_tier_slug"),
        F.upper(F.trim(F.col("duration_sec_tier"))).alias("duration_sec_tier_clean"),
    )
)

result = (
    tagged_2
    .select(
        "is_explicit",
        "distinct_artist_ids",
        "n_is_explicit_true_distance",
        "n_is_explicit_true_plus_total_duration_sec",
        "least_total_duration_sec_n_is_explicit_true",
        "distinct_artist_ids_share_of_distinct_artist_ids",
        "release_date_span_days_avg_3",
        "duration_sec_tier_slug",
        "duration_sec_tier_clean",
        F.lower(F.regexp_replace(F.trim(F.col("duration_sec_tier")), " ", "_")).alias("duration_sec_tier_slug_2"),
    )
    .orderBy(F.col("distinct_artist_ids").desc())
)
