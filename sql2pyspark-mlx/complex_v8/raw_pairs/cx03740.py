from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_row_count_ties = Window.orderBy("row_count")
w_is_skipped_row_count = Window.partitionBy("is_skipped", "row_count")

filtered_plays = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

enriched = (
    filtered_plays
    .filter(F.col("ms_played_filled").between(44500, 380250))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

stats = (
    enriched
    .groupBy("is_skipped")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("is_skipped"), F.col("ms_played"))).alias("ms_played_not_is_skipped"),
        F.max("played_at").alias("last_played_at"),
        F.sum("days_since_played_at").alias("total_days_since_played_at"),
    )
)

ranked = (
    stats
    .select(
        "is_skipped",
        "row_count",
        "ms_played_not_is_skipped",
        "last_played_at",
        F.dense_rank().over(w_all_by_row_count_ties).alias("row_count_dense_rank"),
        F.min("total_days_since_played_at").over(w_is_skipped_row_count).alias("is_skipped_row_count_min_total_days_since_played"),
    )
)

result = (
    ranked
    .select(
        "is_skipped",
        "row_count",
        "is_skipped_row_count_min_total_days_since_played",
        "row_count_dense_rank",
        "ms_played_not_is_skipped",
    )
    .unpivot(
        ids=["is_skipped"],
        values=["row_count", "is_skipped_row_count_min_total_days_since_played", "row_count_dense_rank", "ms_played_not_is_skipped"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
