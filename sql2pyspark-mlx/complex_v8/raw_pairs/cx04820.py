from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .filter(F.col("kwh").between(20.4, 82.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = prepared.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

labeled = (
    sessionized
    .filter(F.col("prev_read_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp"))
    .withColumn("prev_read_at_plus_15m", F.col("prev_read_at") + F.expr("INTERVAL 15 MINUTES"))
)

qualified = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 223.0), "left_anti")
    .filter(F.col("kwh") >= 14.5)
)

result = (
    qualified
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "prev_read_at",
        "session_id",
        F.when(F.col("is_new_session") >= 1, "HIGH").when(F.col("is_new_session") >= 0, "MEDIUM").otherwise("LOW").alias("is_new_session_tier"),
    )
    .orderBy(F.col("session_id").desc(), "id")
    .limit(5)
)
