from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_listener_id_device_by_listen = Window.partitionBy("listener_id", "device").orderBy(F.col("listener_birth_year").desc_nulls_first(), "id")
w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_all_by_listener_birth_year = Window.orderBy(F.col("listener_birth_year").desc(), "id")

filtered_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(8, 14))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_parent = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source_device_label",
        "s.source_clean",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "source_device_label",
        "listener_birth_year",
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_listener_id), F.lit(0)), 4).alias("listener_birth_year_share_of_listener_id"),
        F.round(F.avg("listener_birth_year").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_listener_birth_year"),
        (F.col("listener_birth_year") - F.lag("listener_birth_year").over(w_listener_id_device_by_listen)).alias("listener_birth_year_change"),
        F.lag("listener_birth_year", 3).over(w_all_by_played_at).alias("prev3_listener_birth_year"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.min("listener_birth_year").over(w_listener_id).alias("listener_id_min_listener_birth_year"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_listener_id_track_id), 2).alias("listener_birth_year_vs_listener_id_track_id_avg"),
        F.lag("listener_birth_year").over(w_all_by_listener_birth_year).alias("prev_listener_birth_year"),
    )
)

annotated = (
    with_trends
    .select(
        "*",
        (F.col("played_at") + F.expr("INTERVAL 2 HOURS")).alias("played_at_plus_2h"),
        F.round(F.col("prev_listener_birth_year") / F.nullif(F.col("listener_birth_year"), F.lit(0)), 3).alias("prev_listener_birth_year_per_listener_birth_year"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

totals = (
    annotated
    .groupBy(
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
        F.date_format("played_at_plus_2h", "yyyy-MM").alias("played_at_plus_2h_ym"),
        "distinct_track_id_in_listener_id",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.min("listener_id_track_id_avg_listener_birth_year").alias("min_listener_id_track_id_avg_listener_birth_year"),
    )
)

result = (
    totals
    .select(
        "played_at_ym",
        "played_at_plus_2h_ym",
        "distinct_listener_ids",
        F.when(F.col("distinct_listener_ids") >= 9, "HIGH").when(F.col("distinct_listener_ids") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_listener_ids_tier"),
        (F.floor(F.col("distinct_listener_ids") / 2) * 2).alias("distinct_listener_ids_bucket"),
    )
    .orderBy(
        F.col("distinct_listener_ids_tier").desc(),
        F.col("played_at_ym").asc_nulls_last(),
    )
)
