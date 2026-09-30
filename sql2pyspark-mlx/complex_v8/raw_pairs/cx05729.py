from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier = Window.partitionBy("kwh_tier").orderBy("read_at", "id")
w_kwh_tier_running = Window.partitionBy("kwh_tier").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("kwh") >= 109.3, "large").when(F.col("kwh") >= 30.2, "medium").otherwise("small").alias("kwh_tier"),
    )
)

with_prev = base.withColumn("prev_read_at", F.lag("read_at").over(w_kwh_tier))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_kwh_tier_running))

per_session = (
    sessionized
    .groupBy("kwh_tier", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "kwh_tier",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.to_date(F.col("session_start")).alias("session_start_date"),
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
)
