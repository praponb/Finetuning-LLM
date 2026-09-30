from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_is_skipped_track_id_by_ms_pl = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(F.col("device").isin("desktop", "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

metrics = (
    prepared_plays
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_share_of_track_id_listener_id"),
        F.sum("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_sum_ms_played"),
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
        F.lead("ms_played").over(w_is_skipped_track_id_by_ms_pl).alias("next_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_ms_played"),
    )
)

with_parent = (
    metrics.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select("s.*", F.col("l.plan").alias("listener_plan"))
)

result = (
    with_parent
    .cube("is_skipped", "listener_id")
    .agg(
        F.sum("ms_played").alias("total_ms_played"),
        F.percentile_approx("last_known_ms_played", 0.5).alias("median_last_known_ms_played"),
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
    )
    .orderBy(
        F.col("is_skipped").asc_nulls_last(),
        F.col("listener_id").asc_nulls_last(),
    )
)
