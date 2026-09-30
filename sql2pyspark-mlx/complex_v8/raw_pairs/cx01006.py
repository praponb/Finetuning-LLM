from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("track_id").orderBy(F.col("ms_played_dense_rank").desc(), F.col("id").desc())
w_ms_played_dense_rank = Window.partitionBy("ms_played_dense_rank")
w_all_by_ms_played_change_ties = Window.orderBy(F.col("ms_played_change").desc())
w_all_by_second_ms_played_runn = Window.orderBy("second_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")) & (~F.col("source").isin("playlist", "radio")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

sequenced = (
    cleaned_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "played_at_label",
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_ms_played)).alias("ms_played_change"),
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_full).alias("second_ms_played"),
        F.last("played_at").over(w_all_by_ms_played_full).alias("last_played_at"),
        F.rank().over(w_all_by_ms_played).alias("ms_played_rank"),
        F.lag("played_at").over(w_all_by_ms_played).alias("prev_played_at"),
    )
)

latest_per_key = (
    sequenced
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest_per_key
    .select(
        "id",
        "track_id",
        "played_at",
        "played_at_label",
        "ms_played_dense_rank",
        "ms_played_change",
        "last_played_at",
        "ms_played_rank",
        "prev_played_at",
        F.count(F.lit(1)).over(w_ms_played_dense_rank).alias("rows_in_ms_played_dense_rank"),
        F.round(F.col("ms_played_change") / F.nullif(F.sum("ms_played_change").over(w_ms_played_dense_rank), F.lit(0)), 4).alias("ms_played_change_share_of_ms_played_dense_rank"),
        F.dense_rank().over(w_all_by_ms_played_change_ties).alias("ms_played_change_dense_rank"),
        F.round(F.col("ms_played_change") - F.avg("ms_played_change").over(w_ms_played_dense_rank), 2).alias("ms_played_change_vs_ms_played_dense_rank_avg"),
        F.count(F.lit(1)).over(w_all_by_second_ms_played_runn).alias("running_count"),
    )
)

result = (
    windowed
    .select(
        "id",
        "ms_played_change_vs_ms_played_dense_rank_avg",
        "ms_played_change_share_of_ms_played_dense_rank",
    )
    .unpivot(
        ids=["id"],
        values=["ms_played_change_vs_ms_played_dense_rank_avg", "ms_played_change_share_of_ms_played_dense_rank"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
)
