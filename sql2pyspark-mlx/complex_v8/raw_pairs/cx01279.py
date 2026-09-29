from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reading_read_at_to_date_by_s = Window.partitionBy(F.to_date("reading_read_at")).orderBy("session_end", "site_id", "session_id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_read_at_to_date_by_s_2 = Window.partitionBy(F.to_date("reading_read_at")).orderBy("session_end", "site_id", "session_id", "reading_id").rowsBetween(-6, Window.currentRow)

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
    )
)

with_prev = valid_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

line_level = (
    per_session.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_end",
        "s.session_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

ranked = (
    line_level
    .select(
        "*",
        F.nth_value("session_kwh", 2).over(w_reading_read_at_to_date_by_s).alias("second_session_kwh"),
        F.max("session_kwh").over(w_reading_read_at_to_date_by_s_2).alias("session_kwh_max_7"),
    )
)

result = (
    ranked
    .filter(F.col("site_id").isNotNull())
    .select(
        "site_id",
        "session_id",
        "session_kwh",
        "reading_id",
        "reading_read_at",
        "reading_voltage",
        "second_session_kwh",
        "session_kwh_max_7",
        F.greatest(F.col("reading_voltage"), F.col("session_kwh_max_7")).alias("greatest_reading_voltage_session_kwh_max_7"),
        F.round(F.col("session_kwh") / F.nullif(F.col("session_id"), F.lit(0)), 3).alias("session_kwh_per_session_id"),
    )
    .orderBy("reading_read_at")
)
