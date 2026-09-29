from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_source_by_played_at = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id")

valid = (
    df_plays
    .filter(F.col("source") != "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_windows = (
    valid
    .withColumn("distinct_track_id_in_listener_id", F.size(F.collect_set("track_id").over(w_listener_id)))
    .withColumn("played_at_percent_rank", F.round(F.percent_rank().over(w_source_by_played_at), 3))
)

result = (
    with_windows
    .select(
        "id",
        F.col("ms_played").alias("m_ms_played"),
        F.col("distinct_track_id_in_listener_id").alias("distinct_track_id_in_listener_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_ms_played", "distinct_track_id_in_listener_id_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
