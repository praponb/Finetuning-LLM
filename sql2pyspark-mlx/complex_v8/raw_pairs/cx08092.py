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
        F.when(F.col("voltage") >= 237.5, "A").when(F.col("voltage") >= 222.6, "B").otherwise("C").alias("voltage_tier"),
    )
)

with_prev = scoped.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_site_id_running))

per_session = (
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

enriched_site = (
    per_session.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.session_id",
        "s.session_end",
        "s.event_count",
        "s.duration_min",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

result = (
    enriched_site
    .select(
        "site_id",
        "session_end",
        "event_count",
        "duration_min",
        "site_tariff",
        "site_name",
        (F.col("session_id") + F.col("event_count")).alias("session_id_plus_event_count"),
    )
    .orderBy("event_count", F.col("duration_min").desc())
)
