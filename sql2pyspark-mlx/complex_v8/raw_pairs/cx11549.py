from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_month = Window.partitionBy("played_at_month")
w_listener_id_by_played_at_mon = Window.partitionBy("listener_id").orderBy(F.col("played_at_month").desc(), "id")
w_all_by_played_at_month_prior = Window.orderBy(F.col("played_at_month").desc(), "id").rowsBetween(-6, -1)
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at_month_ties = Window.orderBy("played_at_month")
w_all_by_played_at_month_ties_2 = Window.orderBy(F.col("played_at_month").desc())
w_distinct_listener_id_in_play = Window.partitionBy("distinct_listener_id_in_played_at_month", "listener_id").orderBy(F.col("rows_in_is_skipped").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_track_id_in_is_skip = Window.partitionBy("distinct_track_id_in_is_skipped", "source").orderBy("rows_in_played_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

with_trends = (
    prepared_plays
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.count(F.lit(1)).over(w_played_at_month).alias("rows_in_played_at_month"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at_mon), 3).alias("played_at_month_percent_rank"),
        F.round(F.avg("played_at_month").over(w_all_by_played_at_month_prior), 2).alias("played_at_month_avg_6_prior"),
        F.round(F.col("played_at_month") / F.nullif(F.sum("played_at_month").over(w_is_skipped), F.lit(0)), 4).alias("played_at_month_share_of_is_skipped"),
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("track_id").over(w_is_skipped)).alias("distinct_track_id_in_is_skipped"),
        F.max("played_at_month").over(w_played_at_month).alias("played_at_month_max_played_at_month"),
        F.round(F.percent_rank().over(w_all_by_played_at_month_ties), 3).alias("played_at_month_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_played_at_month_ties_2), 3).alias("played_at_month_cume_dist"),
        F.size(F.collect_set("listener_id").over(w_played_at_month)).alias("distinct_listener_id_in_played_at_month"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "source",
        "source_slug",
        "rows_in_is_skipped",
        "rows_in_played_at_month",
        "played_at_month_avg_6_prior",
        "played_at_month_share_of_is_skipped",
        "played_at_month_max_played_at_month",
        "played_at_month_percent_rank_2",
        "played_at_month_cume_dist",
        F.round(F.col("rows_in_is_skipped") / F.nullif(F.sum("rows_in_is_skipped").over(w_is_skipped), F.lit(0)), 4).alias("rows_in_is_skipped_share_of_is_skipped"),
        F.nth_value("rows_in_played_at_month", 2).over(w_distinct_listener_id_in_play).alias("second_rows_in_played_at_month"),
        F.count(F.lit(1)).over(w_distinct_track_id_in_is_skip).alias("running_count"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped_2"),
    )
)

result = (
    metrics
    .filter(F.month(F.col("played_at")).isin(1, 2))
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "source",
        "rows_in_is_skipped",
        "played_at_month_avg_6_prior",
        "played_at_month_share_of_is_skipped",
        "played_at_month_max_played_at_month",
        "played_at_month_percent_rank_2",
        "rows_in_is_skipped_share_of_is_skipped",
        "second_rows_in_played_at_month",
        "running_count",
        "rows_in_is_skipped_2",
    )
    .orderBy(F.col("rows_in_is_skipped").asc(), "id")
    .limit(10)
)
