from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_is_skipped_true = Window.partitionBy("n_is_skipped_true")
w_track_id_source_by_n_is_skip = Window.partitionBy("track_id", "source").orderBy("n_is_skipped_true", "is_skipped", "track_id", "source").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_is_skipped_true = Window.orderBy(F.col("n_is_skipped_true").desc_nulls_first(), "is_skipped", "track_id", "source")

cleaned_plays = df_plays.filter(F.col("device") == "car")

scored = (
    cleaned_plays
    .filter(F.col("is_skipped"))
    .select(
        "*",
        F.abs(F.col("ms_played") - 112786).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

totals = (
    scored
    .groupBy("is_skipped", "track_id", "source")
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.min("played_at").alias("first_played_at"),
        F.avg("ms_played_bucket").alias("avg_ms_played_bucket_check"),
    )
    .filter(F.col("avg_ms_played_bucket_check") > 123500)
    .drop("avg_ms_played_bucket_check")
)

with_trends = (
    totals
    .select(
        "is_skipped",
        "track_id",
        "source",
        "first_played_at",
        F.count(F.lit(1)).over(w_n_is_skipped_true).alias("rows_in_n_is_skipped_true"),
        F.first("n_is_skipped_true").over(w_track_id_source_by_n_is_skip).alias("first_n_is_skipped_true"),
        F.rank().over(w_all_by_n_is_skipped_true).alias("n_is_skipped_true_rank"),
    )
)

with_details_track = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.is_skipped",
        "s.track_id",
        "s.source",
        "s.rows_in_n_is_skipped_true",
        "s.first_n_is_skipped_true",
        "s.n_is_skipped_true_rank",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

scored_2 = (
    with_details_track
    .filter(F.size(F.col("track_moods")) >= 2)
    .select(
        "*",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref"),
    )
)

is_skipped_label_stats = (
    scored_2
    .groupBy("is_skipped_label")
    .agg(F.max("rows_in_n_is_skipped_true").alias("max_rows_in_n_is_skipped_true"))
)

above_average = (
    scored_2.alias("s")
    .join(is_skipped_label_stats.alias("g"), F.col("g.is_skipped_label") == F.col("s.is_skipped_label"), "inner")
    .filter(F.col("s.rows_in_n_is_skipped_true") == F.col("g.max_rows_in_n_is_skipped_true"))
    .select(
        "s.*",
        "g.max_rows_in_n_is_skipped_true",
        F.round(F.col("s.rows_in_n_is_skipped_true") - F.col("g.max_rows_in_n_is_skipped_true"), 2).alias("rows_in_n_is_skipped_true_minus_max"),
    )
)

result = (
    above_average
    .filter(F.col("source").isNotNull())
    .select(
        "is_skipped",
        "track_id",
        "source",
        "rows_in_n_is_skipped_true",
        "first_n_is_skipped_true",
        "track_moods",
        "track_duration_sec",
        "is_skipped_label",
        "source_ref",
        "max_rows_in_n_is_skipped_true",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy("track_id", "is_skipped", "source")
    .limit(20)
)
