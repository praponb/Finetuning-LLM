from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_source = Window.partitionBy("source")
w_track_id_ms_played_tier_by_d = Window.partitionBy("track_id", "ms_played_tier").orderBy(F.col("days_since_played_at").desc())
w_track_id_ms_played_tier_by_d_2 = Window.partitionBy("track_id", "ms_played_tier").orderBy(F.col("days_since_played_at").desc(), "id")
w_all_by_days_since_played_at = Window.orderBy("days_since_played_at", "id")
w_device = Window.partitionBy("device")
w_all_by_row_count_ties = Window.orderBy("row_count")
w_row_count_n_is_skipped_true = Window.partitionBy("row_count", "n_is_skipped_true")
w_n_is_skipped_true_by_n_track = Window.partitionBy("n_is_skipped_true").orderBy(F.col("n_track_is_explicit_true").desc(), "rows_in_source_tier", "source", "is_skipped").rowsBetween(-2, Window.currentRow)

recent = (
    df_plays
    .filter((F.col("track_id") <= 2) | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.when(F.col("ms_played") >= 220498, "A").when(F.col("ms_played") >= 176302, "B").otherwise("C").alias("ms_played_tier"),
    )
)

with_parent_track = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

ranked = (
    with_parent_track
    .select(
        "*",
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_track_is_explicit), F.lit(0)), 4).alias("track_bpm_share_of_track_is_explicit"),
        F.round(F.col("days_since_played_at") - F.avg("days_since_played_at").over(w_source), 2).alias("days_since_played_at_vs_source_avg"),
        F.rank().over(w_track_id_ms_played_tier_by_d).alias("days_since_played_at_rank"),
        F.lag("listener_birth_year", 2).over(w_track_id_ms_played_tier_by_d_2).alias("prev2_listener_birth_year"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.row_number().over(w_all_by_days_since_played_at).alias("days_since_played_at_seq"),
        F.size(F.collect_set("track_id").over(w_device)).alias("distinct_track_id_in_device"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.ntile(4).over(w_track_id_ms_played_tier_by_d_2).alias("days_since_played_at_quartile"),
        F.row_number().over(w_track_id_ms_played_tier_by_d_2).alias("days_since_played_at_seq_2"),
    )
)

leaders = (
    ranked
    .filter(F.col("days_since_played_at_seq_2") <= 2)
    .drop("days_since_played_at_seq_2")
)

stats = (
    leaders
    .filter(F.col("days_since_played_at_seq") < 7)
    .groupBy(
        F.when(F.col("rows_in_source") >= 14, "large").when(F.col("rows_in_source") >= 5, "medium").otherwise("small").alias("rows_in_source_tier"),
        "source",
        "is_skipped",
    )
    .agg(
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.count(F.when(F.col("track_is_explicit"), 1)).alias("n_track_is_explicit_true"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
)

ranked_2 = (
    stats
    .select(
        "rows_in_source_tier",
        "source",
        "is_skipped",
        "n_track_is_explicit_true",
        F.rank().over(w_all_by_row_count_ties).alias("row_count_rank"),
        F.count(F.lit(1)).over(w_row_count_n_is_skipped_true).alias("rows_in_row_count_n_is_skipped_true"),
        F.max("row_count").over(w_n_is_skipped_true_by_n_track).alias("row_count_max_3"),
    )
)

result = (
    ranked_2
    .select(
        "rows_in_source_tier",
        "source",
        "n_track_is_explicit_true",
        "rows_in_row_count_n_is_skipped_true",
        "row_count_max_3",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.when(F.col("rows_in_source_tier").isin("large", "small"), "group_a").when(F.col("rows_in_source_tier") == "medium", "group_b").otherwise("other").alias("rows_in_source_tier_group"),
    )
)
