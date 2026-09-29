from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id_by_ms_pla_2 = Window.partitionBy("source", "listener_id").orderBy("ms_played_bucket", "id")
w_source_listener_id_by_ms_pla_3 = Window.partitionBy("source", "listener_id").orderBy("ms_played_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .filter((F.col("ms_played") <= 241585) & (F.month(F.col("played_at")).isin(1, 2)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

listener_id_stats = (
    prepared
    .groupBy("listener_id")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

outliers = (
    prepared.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

with_windows = (
    outliers
    .select(
        "*",
        F.round(F.col("source_len") - F.avg("source_len").over(w_source), 2).alias("source_len_vs_source_avg"),
        F.nth_value("ms_played", 2).over(w_source_listener_id_by_ms_pla).alias("second_ms_played"),
        F.dense_rank().over(w_source_listener_id_by_ms_pla_2).alias("ms_played_bucket_dense_rank"),
        F.sum("ms_played_minus_median").over(w_source_listener_id_by_ms_pla_3).alias("running_ms_played_minus_median"),
        F.sum("ms_played").over(w_source_listener_id_by_ms_pla_3).alias("running_ms_played"),
    )
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.avg("running_ms_played").alias("overall_avg_running_ms_played")))
    .filter(F.col("running_ms_played") > F.col("overall_avg_running_ms_played"))
    .drop("overall_avg_running_ms_played")
)

detail_rows = (
    filtered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.hour(F.col("p.played_at")).between(6, 12)), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.played_at_date",
        "s.median_ms_played",
        "s.ms_played_minus_median",
        "s.source_len_vs_source_avg",
        "s.second_ms_played",
        "s.ms_played_bucket_dense_rank",
        "s.running_ms_played",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "play_id",
        F.col("median_ms_played").alias("m_median_ms_played"),
        F.col("ms_played_minus_median").alias("m_ms_played_minus_median"),
        F.col("source_len_vs_source_avg").alias("source_len_vs_source_avg_v"),
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["m_median_ms_played", "m_ms_played_minus_median", "source_len_vs_source_avg_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
