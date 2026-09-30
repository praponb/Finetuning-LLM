from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_minus_avg_f = Window.orderBy("ms_played_minus_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_avg_ms_played_prio = Window.partitionBy("device").orderBy("avg_ms_played", "id").rowsBetween(-6, -1)
w_device_by_avg_ms_played_movi = Window.partitionBy("device").orderBy("avg_ms_played", "id").rowsBetween(-3, Window.currentRow)
w_device = Window.partitionBy("device")

recent_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

source_stats = (
    recent_plays
    .groupBy("source")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

above_average = (
    recent_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

metrics = (
    above_average
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_all_by_ms_played_minus_avg_f).alias("second_ms_played"),
        F.max("ms_played_minus_avg").over(w_device_by_avg_ms_played_prio).alias("ms_played_minus_avg_max_6_prior"),
        F.round(F.avg("avg_ms_played").over(w_device_by_avg_ms_played_movi), 2).alias("avg_ms_played_avg_4"),
        F.round(F.avg("ms_played").over(w_device), 2).alias("device_avg_ms_played"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "played_at_plus_15m",
        "played_at_label",
        "ms_played_minus_avg",
        "second_ms_played",
        "ms_played_minus_avg_max_6_prior",
        "avg_ms_played_avg_4",
        "device_avg_ms_played",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)
