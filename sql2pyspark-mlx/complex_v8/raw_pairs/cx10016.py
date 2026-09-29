from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .filter(F.col("session_start") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "site_id",
        "session_end",
        "n_events",
        F.when(F.col("session_id") >= 4, "large").when(F.col("session_id") >= 3, "medium").otherwise("small").alias("session_id_tier"),
    )
    .orderBy(F.col("site_id").desc(), "session_end")
)
