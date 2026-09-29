from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_len_source_by_days_si = Window.partitionBy("device_len", "source").orderBy(F.col("days_since_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_len_source_by_days_si_2 = Window.partitionBy("device_len", "source").orderBy(F.col("days_since_played_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_device_len_source_by_days_si_3 = Window.partitionBy("device_len", "source").orderBy(F.col("days_since_played_at").desc(), "id")

valid_plays = (
    df_plays
    .filter((F.col("track_id") >= 28) & (F.col("ms_played") > 92277))
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
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

ranked = (
    valid_plays
    .select(
        "*",
        F.last("device_len", ignorenulls=True).over(w_device_len_source_by_days_si).alias("last_known_device_len"),
        F.round(F.avg("ms_played").over(w_device_len_source_by_days_si_2), 2).alias("ms_played_avg_3"),
        F.round(F.cume_dist().over(w_device_len_source_by_days_si_3), 3).alias("days_since_played_at_cume_dist"),
    )
)

with_parent = (
    ranked.alias("s")
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
        "s.device_len",
        "s.source_len",
        "s.days_since_played_at",
        "s.last_known_device_len",
        "s.ms_played_avg_3",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.country").alias("listener_country"),
    )
)

with_parent_2 = (
    with_parent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") > 94)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_len",
        "s.source_len",
        "s.last_known_device_len",
        "s.ms_played_avg_3",
        "s.listener_country",
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    with_parent_2
    .select(
        "id",
        F.col("source_len").alias("m_source_len"),
        F.col("ms_played").alias("m_ms_played"),
        F.col("last_known_device_len").alias("m_last_known_device_len"),
    )
    .unpivot(
        ids=["id"],
        values=["m_source_len", "m_ms_played", "m_last_known_device_len"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
