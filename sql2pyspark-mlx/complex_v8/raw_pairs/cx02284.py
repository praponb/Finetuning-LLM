from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.col("quality").isin("EST", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    per_session
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_kwh",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
    )
)

detail_rows_readings = (
    derived.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_start",
        "s.session_kwh",
        "s.duration_min_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows_readings
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_kwh",
        "duration_min_adj",
        "reading_id",
        "reading_quality",
        "reading_read_at",
        "reading_voltage",
    )
    .orderBy(F.col("reading_quality").desc(), F.col("reading_voltage").desc())
)
