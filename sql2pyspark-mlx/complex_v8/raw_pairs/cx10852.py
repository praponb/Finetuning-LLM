from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh") < 79.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    per_session
    .crossJoin(per_session.agg(F.max("burst_id").alias("overall_max_burst_id")))
    .filter(F.col("burst_id") < F.col("overall_max_burst_id"))
    .drop("overall_max_burst_id")
)

tagged = (
    qualified
    .select(
        "quality",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.quarter(F.col("session_start")).alias("session_start_quarter"),
        F.round(F.col("events") / F.nullif(F.col("burst_id"), F.lit(0)), 3).alias("events_per_burst_id"),
    )
)

result = (
    tagged
    .select(
        "quality",
        "burst_id",
        "session_end",
        "events",
        "quality_or_unknown",
        "events_per_burst_id",
    )
    .orderBy(F.col("events").desc(), F.col("events_per_burst_id").desc())
)
