from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") * 1.07, 2).alias("voltage_adj"),
    )
)

with_prev = cleaned_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = per_session.withColumn("session_start_quarter", F.quarter(F.col("session_start")))

filtered = (
    derived.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 141.6) & (F.col("si.commissioned_on") < F.lit("2024-11-21").cast("date")), "left_anti")
)

result = (
    filtered
    .filter(F.col("duration_min") >= 48.0)
    .select(
        "site_id",
        "session_no",
        "session_end",
        "session_voltage",
        "duration_min",
        F.to_date(F.col("session_end")).alias("session_end_date"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_start")).alias("days_since_session_start"),
    )
    .orderBy(F.col("session_voltage").asc_nulls_last())
)
