from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_track_id_source_by_played_at = Window.partitionBy("track_id", "source").orderBy("played_at", "id")
w_device_track_id_by_listener = Window.partitionBy("device", "track_id").orderBy(F.col("listener_birth_year_minus_median").asc_nulls_last(), "id")
w_track_id = Window.partitionBy("track_id")

valid = (
    df_plays
    .filter(F.col("is_skipped"))
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

metrics = (
    valid
    .select(
        "*",
        F.max("ms_played").over(w_source).alias("source_max_ms_played"),
        F.dense_rank().over(w_track_id_source_by_played_at).alias("played_at_dense_rank"),
        F.row_number().over(w_track_id_source_by_played_at).alias("rn"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
        F.lag("ms_played", 2).over(w_track_id_source_by_played_at).alias("prev2_ms_played"),
    )
)

with_details_listener = (
    metrics.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_max_ms_played",
        "s.rn",
        "s.source_sum_ms_played",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

device_stats = (
    with_details_listener
    .groupBy("device")
    .agg(F.percentile_approx("listener_birth_year", 0.5).alias("median_listener_birth_year"))
)

vs_group = (
    with_details_listener.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.listener_birth_year") >= F.col("g.median_listener_birth_year"))
    .select(
        "s.*",
        "g.median_listener_birth_year",
        F.round(F.col("s.listener_birth_year") - F.col("g.median_listener_birth_year"), 2).alias("listener_birth_year_minus_median"),
    )
)

sequenced = (
    vs_group
    .withColumn("listener_birth_year_minus_median_seq", F.row_number().over(w_device_track_id_by_listener))
    .withColumn("rows_in_track_id", F.count(F.lit(1)).over(w_track_id))
)

result = (
    sequenced
    .select(
        "id",
        "source_max_ms_played",
        "listener_birth_year_minus_median_seq",
        "ms_played",
        "source_sum_ms_played",
    )
    .unpivot(
        ids=["id"],
        values=["source_max_ms_played", "listener_birth_year_minus_median_seq", "ms_played", "source_sum_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .orderBy("id", "metric_name")
)
