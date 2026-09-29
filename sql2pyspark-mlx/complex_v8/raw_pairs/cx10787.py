from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played_filled_movi = Window.orderBy(F.col("ms_played_filled").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_source_listener_id = Window.partitionBy("source", "listener_id")
w_source_listener_id_by_ms_pla_2 = Window.partitionBy("source", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy("ms_played_filled_vs_source_listener_id_avg")

scoped = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

sequenced = (
    scoped
    .select(
        "*",
        F.lag("days_since_played_at").over(w_source_listener_id_by_ms_pla).alias("prev_days_since_played_at"),
        F.sum("ms_played").over(w_all_by_ms_played_filled_movi).alias("ms_played_sum_7"),
        F.round(F.col("ms_played_filled") - F.avg("ms_played_filled").over(w_source_listener_id), 2).alias("ms_played_filled_vs_source_listener_id_avg"),
        F.nth_value("days_since_played_at", 2).over(w_source_listener_id_by_ms_pla_2).alias("second_days_since_played_at"),
    )
)

ranked = (
    sequenced
    .select(
        "id",
        "track_id",
        "ms_played",
        "source",
        "ms_played_filled",
        "days_since_played_at",
        "prev_days_since_played_at",
        "second_days_since_played_at",
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.dense_rank().over(w_source_track_id_by_ms_played).alias("ms_played_filled_vs_source_listener_id_avg_dense"),
    )
)

result = (
    ranked
    .select("id", "prev_days_since_played_at", "days_since_played_at")
    .unpivot(
        ids=["id"],
        values=["prev_days_since_played_at", "days_since_played_at"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric_name")
)
