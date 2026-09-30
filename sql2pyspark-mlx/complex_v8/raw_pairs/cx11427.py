from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_track_id = Window.partitionBy("device", "track_id")

filtered_plays = (
    df_plays
    .filter((F.col("track_id") <= 5) | (F.hour(F.col("played_at")).between(8, 13)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

derived = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source_len",
        F.least(F.col("ms_played"), F.col("source_len")).alias("least_ms_played_source_len"),
        F.coalesce(F.col("source_len"), F.lit(0)).alias("source_len_filled"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "source_len",
        "least_ms_played_source_len",
        "ms_played_filled",
        F.size(F.collect_set("listener_id").over(w_device_track_id)).alias("distinct_listener_id_in_device_track_id"),
        F.max("ms_played_filled").over(w_device_track_id).alias("device_track_id_max_ms_played_filled"),
        F.max("source_len").over(w_device_track_id).alias("device_track_id_max_source_len"),
    )
)

enriched = (
    with_trends.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.plan") != "premium")
    .select(
        "s.*",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ms_played",
        "device_track_id_max_source_len",
        "distinct_listener_id_in_device_track_id",
    )
    .unpivot(
        ids=["id"],
        values=["ms_played", "device_track_id_max_source_len", "distinct_listener_id_in_device_track_id"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
