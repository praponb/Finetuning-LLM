from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.least(F.col("kwh"), F.col("voltage")).alias("least_kwh_voltage"),
    )
)

with_prev = scoped.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

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
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    per_session
    .select(
        "site_id",
        "session_id",
        "session_end",
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
        F.least(F.col("n_events"), F.col("duration_min")).alias("least_n_events_duration_min"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled_2"),
    )
)

detail_rows_readings = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_end",
        "s.duration_min_filled",
        "s.least_n_events_duration_min",
        "s.duration_min_filled_2",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
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
        "reading_kwh",
        "least_n_events_duration_min",
        "duration_min_filled_2",
    )
    .unpivot(
        ids=["site_id", "session_id"],
        values=["reading_kwh", "least_n_events_duration_min", "duration_min_filled_2"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("site_id", "session_id", "field")
)
