from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 221.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = cleaned_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

rollup_base = (
    sessions
    .groupBy(F.date_trunc("month", "session_start").alias("session_start_month"))
    .agg(
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.min("session_start").alias("first_session_start"),
        F.max("session_voltage").alias("max_session_voltage"),
        F.count("*").alias("row_count"),
    )
)

result = (
    rollup_base
    .filter(F.month(F.col("session_start_month")).isin(1, 3))
    .select(
        "session_start_month",
        "first_session_start",
        "max_session_voltage",
        "row_count",
        F.date_trunc("hour", F.col("session_start_month")).alias("session_start_month_hour"),
    )
    .orderBy("session_start_month", F.col("first_session_start").asc_nulls_last())
)
