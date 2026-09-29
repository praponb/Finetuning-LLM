from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_track_id = Window.partitionBy(F.year("played_at"), "track_id")
w_source_by_ms_played_ties = Window.partitionBy("source").orderBy("ms_played")
w_all_by_ms_played_filled = Window.orderBy("ms_played_filled", "id")

scoped_plays = (
    df_plays
    .filter((F.col("source").isin("playlist", "search", "radio")) & (~F.col("device").isin("tv", "car")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_activity = (
    scoped_plays
    .crossJoin(scoped_plays.agg(F.min("ms_played").alias("overall_min_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_min_ms_played"))
    .drop("overall_min_ms_played")
)

annotated = (
    with_activity
    .select(
        "*",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.count(F.lit(1)).over(w_played_at_year_track_id).alias("rows_in_played_at_year_track_id"),
        F.round(F.cume_dist().over(w_source_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_ms_played_filled), 3).alias("ms_played_filled_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "source",
        "device_prefix",
        "ms_played_filled",
        "rows_in_played_at_year_track_id",
        "ms_played_filled_percent_rank",
        (F.floor(F.col("ms_played_filled") / 50000) * 50000).alias("ms_played_filled_bucket"),
    )
    .orderBy(F.col("ms_played_filled").asc())
)
