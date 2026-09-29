from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = df_meter_readings.filter(F.col("voltage").between(220.2, 243.1))

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.min("n_events").alias("overall_min_n_events")))
    .filter(F.col("n_events") > F.col("overall_min_n_events"))
    .drop("overall_min_n_events")
)

labeled = (
    with_activity
    .select(
        "*",
        F.coalesce(F.col("session_no"), F.lit(0)).alias("session_no_filled"),
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
    )
)

result = (
    labeled
    .groupBy("session_start")
    .pivot("quality", ["BAD", "OK", "EST"])
    .agg(F.sum("n_events").alias("total"), F.count("n_events").alias("n"))
)
