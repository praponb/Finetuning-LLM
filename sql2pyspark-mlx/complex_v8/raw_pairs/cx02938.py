from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year = Window.partitionBy(F.year("played_at"))
w_device_source_by_ms_played_m = Window.partitionBy("device", "source").orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

sequenced = (
    prepared_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device_or_unknown",
        "is_skipped_label",
        F.max("ms_played").over(w_played_at_year).alias("played_at_year_max_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_year), 2).alias("ms_played_vs_played_at_year_avg"),
        F.round(F.avg("ms_played").over(w_device_source_by_ms_played_m), 2).alias("ms_played_avg_2"),
        F.sum("ms_played").over(w_played_at_year).alias("played_at_year_sum_ms_played"),
        F.last("played_at_plus_1d", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at_plus_1d"),
    )
)

result = (
    sequenced
    .rollup("track_id", "listener_id")
    .agg(
        F.round(F.avg("played_at_year_max_ms_played"), 2).alias("avg_played_at_year_max_ms_played"),
        F.round(F.sum("ms_played_avg_2") / F.nullif(F.sum("ms_played_vs_played_at_year_avg"), F.lit(0)), 3).alias("ms_played_avg_2_to_ms_played_vs_played_at_year_a"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
