from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_is_explicit_true_release_d = Window.partitionBy("n_is_explicit_true", F.date_trunc("month", "release_date_month")).orderBy("n_is_explicit_true", "release_date_month")
w_n_is_explicit_true_release_d_2 = Window.partitionBy("n_is_explicit_true", F.date_trunc("month", "release_date_month")).orderBy("n_is_explicit_true", "release_date_month").rowsBetween(-1, -1)
w_n_is_explicit_true = Window.partitionBy("n_is_explicit_true")

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 357)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

screened = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.source").isin("search", "playlist")), "left_anti")
)

summary = (
    screened
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
        F.sum(F.when(~F.col("is_explicit"), F.col("days_since_release_date")).otherwise(0)).alias("days_since_release_date_not_is_explicit"),
    )
)

with_trends = (
    summary
    .select(
        "release_date_month",
        "n_is_explicit_true",
        "days_since_release_date_not_is_explicit",
        F.round((F.unix_timestamp("release_date_month") - F.unix_timestamp(F.lag("release_date_month").over(w_n_is_explicit_true_release_d))) / 60, 1).alias("minutes_since_prev_release_date_month"),
        F.lead("release_date_month", 3).over(w_n_is_explicit_true_release_d).alias("next3_release_date_month"),
        F.round(F.avg("n_is_explicit_true").over(w_n_is_explicit_true_release_d_2), 2).alias("n_is_explicit_true_avg_1_prior"),
        F.lag("release_date_month", 2).over(w_n_is_explicit_true_release_d).alias("prev2_release_date_month"),
        F.round(F.col("n_is_explicit_true") - F.avg("n_is_explicit_true").over(w_n_is_explicit_true), 2).alias("n_is_explicit_true_vs_n_is_explicit_true_avg"),
        F.round(F.avg("n_is_explicit_true").over(w_n_is_explicit_true), 2).alias("n_is_explicit_true_avg_n_is_explicit_true"),
    )
)

result = (
    with_trends
    .select(
        "release_date_month",
        "n_is_explicit_true",
        "days_since_release_date_not_is_explicit",
        "minutes_since_prev_release_date_month",
        "next3_release_date_month",
        "n_is_explicit_true_avg_1_prior",
        "prev2_release_date_month",
        "n_is_explicit_true_vs_n_is_explicit_true_avg",
        "n_is_explicit_true_avg_n_is_explicit_true",
        F.date_format(F.col("release_date_month"), "yyyy-MM-dd HH:00").alias("release_date_month_label"),
    )
    .orderBy(
        F.col("prev2_release_date_month").desc(),
        F.col("n_is_explicit_true_vs_n_is_explicit_true_avg").asc_nulls_last(),
        "release_date_month",
    )
    .limit(10)
)
