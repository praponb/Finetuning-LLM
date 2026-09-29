from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_label = Window.partitionBy("is_skipped_label").orderBy("played_at", "id")
w_is_skipped_label_running = Window.partitionBy("is_skipped_label").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_count_prior_2 = Window.orderBy(F.col("event_count").desc(), "is_skipped_label", "burst_id").rowsBetween(-2, -1)
w_is_skipped_label_session_end = Window.partitionBy("is_skipped_label", F.year("session_end"))
w_event_count_burst_id_by_even = Window.partitionBy("event_count", "burst_id").orderBy(F.col("event_count").desc(), "is_skipped_label", "burst_id")

filtered_plays = (
    df_plays
    .filter(F.col("source") != "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

with_prev = filtered_plays.withColumn("prev_played_at", F.lag("played_at").over(w_is_skipped_label))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_is_skipped_label_running))

per_session = (
    sessionized
    .groupBy("is_skipped_label", "burst_id")
    .agg(
        F.min("played_at").alias("session_start"),
        F.max("played_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("played_at")) - F.unix_timestamp(F.min("played_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    per_session
    .select(
        "is_skipped_label",
        "burst_id",
        "duration_min",
        F.round(F.avg("event_count").over(w_all_by_event_count_prior_2), 2).alias("event_count_avg_2_prior"),
        F.count(F.lit(1)).over(w_is_skipped_label_session_end).alias("rows_in_is_skipped_label_session_end_year"),
        F.lag("session_start").over(w_event_count_burst_id_by_even).alias("prev_session_start"),
        F.round(F.col("event_count") / F.nullif(F.sum("event_count").over(w_is_skipped_label_session_end), F.lit(0)), 4).alias("event_count_share_of_is_skipped_label_session_en"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["burst_id", "is_skipped_label"], ["burst_id"]],
        "burst_id", "is_skipped_label",
    )
    .agg(
        F.countDistinct("is_skipped_label").alias("distinct_is_skipped_labels"),
        F.count("*").alias("row_count"),
        F.max("burst_id").alias("max_burst_id"),
    )
    .select(
        "burst_id",
        F.coalesce(F.col("is_skipped_label"), F.lit("ALL")).alias("is_skipped_label"),
        "distinct_is_skipped_labels",
        "row_count",
        "max_burst_id",
    )
)
