from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_plan_listener_id = Window.partitionBy("listener_plan", "listener_id")
w_device_len_2_by_listener_sig = Window.partitionBy("device_len_2").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_source = Window.partitionBy("listener_plan", "source")
w_all_by_min_device_len_2_so_f = Window.orderBy("min_device_len_2_so_far", "id")

prepared = (
    df_plays
    .filter(F.col("source").isin("album", "search", "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("desktop", "phone", "speaker")) & (F.month(F.col("p.played_at")).isin(1, 3)), "left_anti")
)

annotated = (
    without_matches
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len_2"),
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        (F.col("device_len") + F.col("ms_played")).alias("device_len_plus_ms_played"),
    )
)

tagged = (
    annotated
    .filter(F.col("listener_id").isNotNull())
    .withColumn("source_clean", F.initcap(F.trim(F.col("source"))))
)

enriched_listener = (
    tagged.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.device_len_2",
        "s.device_len_plus_ms_played",
        "s.source_clean",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

metrics = (
    enriched_listener
    .select(
        "id",
        "listener_id",
        "device",
        "source",
        "device_len_2",
        "device_len_plus_ms_played",
        "listener_plan",
        "listener_signup_date",
        F.round(F.col("device_len_2") - F.avg("device_len_2").over(w_listener_plan_listener_id), 2).alias("device_len_2_vs_listener_plan_listener_id_avg"),
        F.min("device_len_2").over(w_device_len_2_by_listener_sig).alias("min_device_len_2_so_far"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "listener_id",
        "source",
        "device_len_2",
        "listener_plan",
        "device_len_2_vs_listener_plan_listener_id_avg",
        F.round(F.col("device_len_plus_ms_played") / F.nullif(F.sum("device_len_plus_ms_played").over(w_listener_plan_source), F.lit(0)), 4).alias("device_len_plus_ms_played_share_of_listener_plan"),
        F.lag("device_len_2", 2, 0).over(w_all_by_min_device_len_2_so_f).alias("prev2_device_len_2"),
        F.ntile(3).over(w_all_by_min_device_len_2_so_f).alias("min_device_len_2_so_far_tercile"),
        F.round(F.col("device_len_plus_ms_played") - F.avg("device_len_plus_ms_played").over(w_listener_plan_source), 2).alias("device_len_plus_ms_played_vs_listener_plan_sourc"),
        (F.col("min_device_len_2_so_far") - F.lag("min_device_len_2_so_far").over(w_all_by_min_device_len_2_so_f)).alias("min_device_len_2_so_far_change"),
        F.round(F.percent_rank().over(w_all_by_min_device_len_2_so_f), 3).alias("min_device_len_2_so_far_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "source",
        "device_len_2",
        "listener_plan",
        "device_len_2_vs_listener_plan_listener_id_avg",
        "device_len_plus_ms_played_share_of_listener_plan",
        "min_device_len_2_so_far_change",
        "min_device_len_2_so_far_percent_rank",
    )
    .orderBy(
        F.col("device_len_2_vs_listener_plan_listener_id_avg").asc_nulls_last(),
        F.col("listener_id").desc(),
        "id",
    )
    .limit(20)
)
