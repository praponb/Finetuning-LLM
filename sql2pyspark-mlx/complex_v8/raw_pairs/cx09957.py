from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-3, -1)
w_artist_id_release_date_year_2 = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit_by_duration_sec_4 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec_avg_3_prior_per_next2_duration_sec").desc(), "id")
w_release_date_quintile_by_fir = Window.partitionBy("release_date_quintile").orderBy(F.col("first_release_date").desc(), "id")
w_release_date_quintile_by_dur = Window.partitionBy("release_date_quintile").orderBy(F.col("duration_sec_avg_3_prior").desc(), "id")
w_release_date_quintile_by_dur_2 = Window.partitionBy("release_date_quintile").orderBy(F.col("duration_sec_avg_3_prior").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_quintile_by_fir_2 = Window.partitionBy("release_date_quintile").orderBy(F.col("first_release_date").desc(), "id").rowsBetween(-2, -1)

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 353)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

ranked = (
    cleaned_tracks
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_duration_sec"),
        F.rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_rank"),
        F.max("duration_sec").over(w_is_explicit_by_duration_sec_2).alias("duration_sec_max_2"),
        F.first("release_date").over(w_artist_id_release_date_year).alias("first_release_date"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.sum("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_duration_sec"),
        F.lead("release_date").over(w_is_explicit_by_release_date).alias("next_release_date"),
        F.row_number().over(w_is_explicit_by_duration_sec_3).alias("seq_no"),
        F.ntile(5).over(w_is_explicit_by_release_date).alias("release_date_quintile"),
        F.last("release_date").over(w_artist_id_release_date_year).alias("last_release_date"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.round(F.avg("duration_sec").over(w_is_explicit_by_release_date_2), 2).alias("duration_sec_avg_3_prior"),
        F.lag("duration_sec").over(w_artist_id_release_date_year_2).alias("prev_duration_sec"),
        F.lead("duration_sec", 2, 0).over(w_artist_id_release_date_year_2).alias("next2_duration_sec"),
    )
)

labeled = (
    ranked
    .filter(F.col("artist_id_is_explicit_avg_duration_sec") > 156.0)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        "title_ref",
        "duration_sec_max_2",
        "first_release_date",
        "seq_no",
        "release_date_quintile",
        "last_release_date",
        "duration_sec_avg_3_prior",
        "prev_duration_sec",
        "next2_duration_sec",
        F.when(F.col("release_date_quintile") >= 8, "A").when(F.col("release_date_quintile") >= 4, "B").otherwise("C").alias("release_date_quintile_tier"),
        F.round(F.col("duration_sec_avg_3_prior") / F.nullif(F.col("next2_duration_sec"), F.lit(0)), 3).alias("duration_sec_avg_3_prior_per_next2_duration_sec"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        (F.col("release_date_quintile") - F.lag("release_date_quintile").over(w_is_explicit_by_duration_sec_4)).alias("release_date_quintile_change"),
        F.row_number().over(w_release_date_quintile_by_fir).alias("first_release_date_seq"),
        F.round(F.percent_rank().over(w_release_date_quintile_by_dur), 3).alias("duration_sec_avg_3_prior_percent_rank"),
        F.last("duration_sec_max_2").over(w_release_date_quintile_by_dur_2).alias("last_duration_sec_max_2"),
        F.round(F.avg("duration_sec_avg_3_prior").over(w_release_date_quintile_by_fir_2), 2).alias("duration_sec_avg_3_prior_avg_2_prior"),
        (F.col("duration_sec_avg_3_prior") - F.lag("duration_sec_avg_3_prior").over(w_release_date_quintile_by_dur)).alias("duration_sec_avg_3_prior_change"),
        F.row_number().over(w_is_explicit_by_duration_sec_4).alias("rn"),
    )
)

per_group = (
    metrics
    .filter(F.col("first_release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-04-02").cast("date")))
    .groupBy(
        "rn",
        F.date_format("first_release_date", "yyyy-MM").alias("first_release_date_ym"),
        "release_date_quintile_tier",
    )
    .agg(
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(F.avg("rn"), 2).alias("avg_rn"),
        F.count("*").alias("cnt"),
        F.countDistinct("release_date_quintile_tier").alias("distinct_release_date_quintile_tiers"),
        F.percentile_approx("duration_sec_avg_3_prior_avg_2_prior", 0.5).alias("median_duration_sec_avg_3_prior_avg_2_prior"),
    )
)

result = (
    per_group
    .filter(F.col("median_duration_sec_avg_3_prior_avg_2_prior") <= 354.0)
    .select(
        "rn",
        "release_date_quintile_tier",
        "distinct_artist_ids",
        "avg_rn",
        "cnt",
        "distinct_release_date_quintile_tiers",
        "median_duration_sec_avg_3_prior_avg_2_prior",
        F.length(F.trim(F.col("first_release_date_ym"))).alias("first_release_date_ym_len"),
        F.round(F.col("avg_rn") * 0.25, 2).alias("avg_rn_adj"),
    )
    .orderBy(F.col("avg_rn").desc(), "distinct_artist_ids")
)
