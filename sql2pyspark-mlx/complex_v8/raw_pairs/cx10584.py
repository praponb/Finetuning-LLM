from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_n_is_explicit_true = Window.partitionBy("artist_id", "n_is_explicit_true").orderBy(F.col("distinct_bpm_tiers").desc(), "artist_id", "release_date_month_month")
w_all_by_n_is_explicit_true_ti = Window.orderBy("n_is_explicit_true")
w_distinct_bpm_tiers = Window.partitionBy("distinct_bpm_tiers")

scoped = (
    df_tracks
    .filter(F.col("release_date").between(F.lit("2024-09-26").cast("date"), F.lit("2025-11-14").cast("date")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.when(F.col("bpm") >= 127, "HIGH").when(F.col("bpm") >= 82, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

totals = (
    scoped
    .filter(F.col("is_explicit_label").isNotNull())
    .groupBy(
        "artist_id",
        F.date_trunc("month", "release_date_month").alias("release_date_month_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_not_is_explicit"),
        F.countDistinct("bpm_tier").alias("distinct_bpm_tiers"),
        F.sum(F.when(F.col("is_explicit"), 1).otherwise(0)).alias("n_is_explicit_true"),
        F.round(100 * F.count(F.when(F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_is_explicit_true"),
    )
)

windowed = (
    totals
    .select(
        "*",
        F.ntile(3).over(w_artist_id_n_is_explicit_true).alias("distinct_bpm_tiers_tercile"),
        F.lag("distinct_bpm_tiers").over(w_artist_id_n_is_explicit_true).alias("prev_distinct_bpm_tiers"),
        F.round(F.percent_rank().over(w_all_by_n_is_explicit_true_ti), 3).alias("n_is_explicit_true_percent_rank"),
        F.round(F.col("n_is_explicit_true") / F.nullif(F.sum("n_is_explicit_true").over(w_distinct_bpm_tiers), F.lit(0)), 4).alias("n_is_explicit_true_share_of_distinct_bpm_tiers"),
    )
)

with_details = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.artist_id",
        "s.release_date_month_month",
        "s.pct_not_is_explicit",
        "s.distinct_bpm_tiers",
        "s.n_is_explicit_true",
        "s.pct_is_explicit_true",
        "s.prev_distinct_bpm_tiers",
        "s.n_is_explicit_true_percent_rank",
        "s.n_is_explicit_true_share_of_distinct_bpm_tiers",
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_details
    .select(
        "artist_id",
        "release_date_month_month",
        "n_is_explicit_true_percent_rank",
        "n_is_explicit_true_share_of_distinct_bpm_tiers",
        "pct_is_explicit_true",
    )
    .unpivot(
        ids=["artist_id", "release_date_month_month"],
        values=["n_is_explicit_true_percent_rank", "n_is_explicit_true_share_of_distinct_bpm_tiers", "pct_is_explicit_true"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
