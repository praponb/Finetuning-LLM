from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_kwh_tier = Window.partitionBy("kwh_tier").orderBy("read_at", "id")
w_kwh_tier_running = Window.partitionBy("kwh_tier").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 30.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("kwh") >= 81.9, "HIGH").when(F.col("kwh") >= 35.9, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_kwh_tier))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_kwh_tier_running))

per_session = (
    sessionized
    .groupBy("kwh_tier", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    per_session
    .filter(F.col("session_end") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "kwh_tier",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "session_kwh",
        "duration_min",
        F.when(F.col("kwh_tier").isin("LOW", "MEDIUM"), "group_a").when(F.col("kwh_tier") == "HIGH", "group_b").otherwise("other").alias("kwh_tier_group"),
        (F.col("session_start") + F.expr("INTERVAL 15 MINUTES")).alias("session_start_plus_15m"),
    )
)

result = (
    annotated
    .select(
        "kwh_tier",
        "session_start",
        "events",
        "session_kwh",
        "kwh_tier_group",
    )
    .orderBy("kwh_tier_group")
)
