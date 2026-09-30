from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

with_prev = prepared_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

result = (
    sessionized
    .filter(F.col("starts_session").between(0, 1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "starts_session",
        "session_no",
        F.round((F.unix_timestamp(F.col("read_at")) - F.unix_timestamp(F.col("prev_read_at"))) / 3600, 1).alias("hours_prev_read_at_to_read_at"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
    .orderBy("read_at")
)
