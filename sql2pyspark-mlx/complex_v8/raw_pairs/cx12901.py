from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_device_len_ti = Window.partitionBy("listener_id").orderBy("device_len")
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_device_len_ru = Window.partitionBy("listener_id").orderBy("device_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(F.col("source").isNotNull())
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
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

ranked = (
    cleaned_plays
    .select(
        "*",
        F.dense_rank().over(w_listener_id_by_device_len_ti).alias("device_len_dense_rank"),
        F.size(F.collect_set("source").over(w_listener_id)).alias("distinct_source_in_listener_id"),
        F.first("played_at_date", ignorenulls=True).over(w_listener_id_by_device_len_ru).alias("first_known_played_at_date"),
    )
)

combined_details = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .filter(F.col("t.title").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.device_len",
        "s.played_at_date",
        "s.device_len_dense_rank",
        "s.distinct_source_in_listener_id",
        "s.first_known_played_at_date",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    combined_details
    .select(
        "id",
        F.col("device_len_dense_rank").alias("device_len_dense_rank_v"),
        F.col("device_len").alias("m_device_len"),
        F.col("track_bpm").alias("m_track_bpm"),
        F.col("distinct_source_in_listener_id").alias("m_distinct_source_in_listener_id"),
    )
    .unpivot(
        ids=["id"],
        values=["device_len_dense_rank_v", "m_device_len", "m_track_bpm", "m_distinct_source_in_listener_id"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
