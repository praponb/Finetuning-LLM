from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

summary = (
    per_session
    .filter(F.col("burst_id") > 3)
    .groupBy(
        "burst_id",
        "site_id",
        F.date_trunc("month", "session_start").alias("session_start_month"),
    )
    .agg(
        F.round(F.stddev("duration_min"), 2).alias("stddev_duration_min"),
        F.min("session_end").alias("first_session_end"),
        F.max("duration_min").alias("max_duration_min_check"),
    )
    .filter(F.col("max_duration_min_check") > 24.0)
    .drop("max_duration_min_check")
)

result = (
    summary
    .groupingSets(
        [["burst_id", "site_id"], ["burst_id"]],
        "burst_id", "site_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.percentile_approx("burst_id", 0.5).alias("median_burst_id"),
        F.round(F.sum("burst_id") / F.nullif(F.sum("stddev_duration_min"), F.lit(0)), 3).alias("burst_id_to_stddev_duration_min_ratio"),
        F.max("session_start_month").alias("last_session_start_month"),
    )
    .orderBy(F.col("burst_id").asc_nulls_last(), F.col("site_id").asc_nulls_last())
)
