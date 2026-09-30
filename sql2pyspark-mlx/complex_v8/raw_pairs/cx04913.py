from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device").orderBy("played_at", "id")
w_device_running = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_prev = scoped.withColumn("prev_played_at", F.lag("played_at").over(w_device))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_device_running))

derived = (
    sessionized
    .filter(~F.col("is_skipped"))
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("prev_played_at")) - F.unix_timestamp(F.col("played_at"))) / 3600, 1).alias("hours_played_at_to_prev_played_at"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

result = (
    derived
    .groupingSets(
        [["source_or_unknown", "starts_session", "device"], ["source_or_unknown"], ["starts_session"], []],
        "source_or_unknown", "starts_session", "device",
    )
    .agg(
        F.round(F.stddev("starts_session"), 2).alias("stddev_starts_session"),
        F.round(F.sum("starts_session") / F.nullif(F.sum("session_no"), F.lit(0)), 3).alias("starts_session_to_session_no_ratio"),
    )
    .select(
        F.coalesce(F.col("source_or_unknown"), F.lit("ALL")).alias("source_or_unknown"),
        "starts_session",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "stddev_starts_session",
        "starts_session_to_session_no_ratio",
    )
    .orderBy(
        F.col("source_or_unknown").asc_nulls_last(),
        F.col("starts_session").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
