from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)
w_device_listener_id_by_ms_pla_2 = Window.partitionBy("device", "listener_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_is_skipped_by_ms_played_movi = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")

prepared = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played") > 98052))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

metrics = (
    prepared
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_device_listener_id_by_ms_pla), 2).alias("ms_played_avg_1_prior"),
        F.round(F.avg("ms_played").over(w_device_listener_id_by_ms_pla_2), 2).alias("ms_played_avg_7"),
        F.max("ms_played").over(w_is_skipped_by_ms_played_movi).alias("ms_played_max_2"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
    )
)

device_stats = (
    metrics
    .groupBy("device")
    .agg(F.max("ms_played_avg_1_prior").alias("max_ms_played_avg_1_prior"))
)

outliers = (
    metrics.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played_avg_1_prior") == F.col("g.max_ms_played_avg_1_prior"))
    .select(
        "s.*",
        "g.max_ms_played_avg_1_prior",
        F.round(F.col("s.ms_played_avg_1_prior") - F.col("g.max_ms_played_avg_1_prior"), 2).alias("ms_played_avg_1_prior_minus_max"),
    )
)

line_level = (
    outliers.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.device_clean",
        "s.ms_played_avg_7",
        "s.ms_played_max_2",
        "s.listener_id_sum_ms_played",
        "s.max_ms_played_avg_1_prior",
        "s.ms_played_avg_1_prior_minus_max",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    line_level
    .cube("track_id", "listener_id")
    .agg(F.countDistinct("listener_id").alias("distinct_listener_ids"))
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
