from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy(F.col("played_at").asc_nulls_last())
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_all_by_source_len_ties = Window.orderBy(F.col("source_len").desc())
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_is_skipped_by_played_at_movi = Window.partitionBy("is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_source_len_running = Window.orderBy(F.col("source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_source_len_sha = Window.partitionBy("is_skipped").orderBy(F.col("source_len_share_of_track_id_is_skipped").desc(), "id").rowsBetween(-3, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_is_skipped_by_source_len_sha_2 = Window.partitionBy("is_skipped").orderBy(F.col("source_len_share_of_track_id_is_skipped").desc(), "id")
w_is_skipped_by_source_len_sha_3 = Window.partitionBy("is_skipped").orderBy(F.col("source_len_share_of_track_id_is_skipped").desc())

recent_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source") != "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_details = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.dense_rank().over(w_is_skipped_by_played_at_ties).alias("played_at_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_is_skipped), 2).alias("ms_played_vs_track_id_is_skipped_avg"),
        F.round(F.avg("ms_played").over(w_track_id_is_skipped), 2).alias("track_id_is_skipped_avg_ms_played"),
        F.round(F.col("source_len") / F.nullif(F.sum("source_len").over(w_track_id_is_skipped), F.lit(0)), 4).alias("source_len_share_of_track_id_is_skipped"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_track_id_is_skipped"),
        F.dense_rank().over(w_all_by_source_len_ties).alias("source_len_dense_rank"),
        F.lag("played_at", 2).over(w_is_skipped_by_played_at).alias("prev2_played_at"),
        F.round(F.avg("source_len").over(w_is_skipped_by_played_at_movi), 2).alias("source_len_avg_7"),
        F.sum("source_len").over(w_all_by_source_len_running).alias("running_source_len"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("source_len_dense_rank") <= 2)
    .drop("source_len_dense_rank")
)

ranked = (
    first_rows
    .select(
        "*",
        F.max("ms_played").over(w_is_skipped_by_source_len_sha).alias("ms_played_max_4"),
        F.round(F.col("played_at_dense_rank") / F.nullif(F.sum("played_at_dense_rank").over(w_listener_id), F.lit(0)), 4).alias("played_at_dense_rank_share_of_listener_id"),
        F.round(F.cume_dist().over(w_all_by_played_at), 3).alias("played_at_cume_dist"),
        (F.col("running_source_len") - F.lag("running_source_len").over(w_all_by_played_at)).alias("running_source_len_change"),
        F.ntile(4).over(w_is_skipped_by_source_len_sha_2).alias("source_len_share_of_track_id_is_skipped_quartile"),
        F.round(F.percent_rank().over(w_is_skipped_by_source_len_sha_3), 3).alias("source_len_share_of_track_id_is_skipped_percent_"),
    )
)

result = (
    ranked
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source_len",
        "listener_plan",
        "listener_interests",
        "played_at_dense_rank",
        "track_id_is_skipped_avg_ms_played",
        "source_len_share_of_track_id_is_skipped",
        "ms_played_share_of_track_id_is_skipped",
        "prev2_played_at",
        "source_len_avg_7",
        "ms_played_max_4",
        "played_at_dense_rank_share_of_listener_id",
        "played_at_cume_dist",
        "running_source_len_change",
        "source_len_share_of_track_id_is_skipped_quartile",
        "source_len_share_of_track_id_is_skipped_percent_",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.when(F.col("source_len_avg_7") >= 11.6, "A").when(F.col("source_len_avg_7") >= 4.4, "B").otherwise("C").alias("source_len_avg_7_tier"),
    )
    .orderBy(
        F.col("source_len_share_of_track_id_is_skipped_percent_").desc_nulls_first(),
    )
)
