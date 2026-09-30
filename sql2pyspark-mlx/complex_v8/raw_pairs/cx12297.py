from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_moving_3 = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(-3, Window.currentRow)
w_device_listener_country_by_m = Window.partitionBy("device", "listener_country").orderBy(F.col("ms_played_minus_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_listener_country_by_m_2 = Window.partitionBy("device", "listener_country").orderBy(F.col("ms_played_minus_avg").desc(), "id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
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

with_details_listener = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.country").alias("listener_country"))
)

listener_id_stats = (
    with_details_listener
    .groupBy("listener_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    with_details_listener.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

metrics = (
    vs_group
    .select(
        "*",
        F.round(F.avg("ms_played_minus_avg").over(w_all_by_ms_played_moving_3), 2).alias("ms_played_minus_avg_avg_4"),
        F.first("avg_ms_played", ignorenulls=True).over(w_device_listener_country_by_m).alias("first_known_avg_ms_played"),
        F.lead("played_at", 3).over(w_device_listener_country_by_m_2).alias("next3_played_at"),
        F.max("ms_played").over(w_device_listener_country_by_m).alias("max_ms_played_so_far"),
        F.min("ms_played_minus_avg").over(w_all_by_ms_played_running).alias("min_ms_played_minus_avg_so_far"),
    )
)

result = (
    metrics
    .select("id", "ms_played", "max_ms_played_so_far")
    .unpivot(
        ids=["id"],
        values=["ms_played", "max_ms_played_so_far"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)
