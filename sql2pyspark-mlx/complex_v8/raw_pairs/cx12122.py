from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_quality_running))

per_session = (
    sessionized
    .groupBy("quality", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .groupingSets(
        [["quality", "events", "burst_id"], ["quality"], ["events"], []],
        "quality", "events", "burst_id",
    )
    .agg(
        F.grouping("quality").alias("is_total_quality"),
        F.round(F.stddev("events"), 2).alias("stddev_events"),
        F.count(F.when(~F.col("quality").isin("EST", "BAD"), 1)).alias("n_quality_not_in_est_bad"),
        F.sum(F.when(F.col("quality").isin("OK", "EST"), F.col("events"))).alias("events_quality_in_ok_est"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "events",
        "burst_id",
        "is_total_quality",
        "stddev_events",
        "n_quality_not_in_est_bad",
        "events_quality_in_ok_est",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("events").asc_nulls_last(),
        F.col("burst_id").asc_nulls_last(),
    )
)
