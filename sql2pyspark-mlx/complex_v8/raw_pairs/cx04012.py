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
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("kwh").alias("session_kwh"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .filter(F.col("burst_id") > 3)
    .select(
        "site_id",
        "burst_id",
        "session_end",
        "session_kwh",
        F.coalesce(F.col("session_kwh"), F.lit(0.0)).alias("session_kwh_filled"),
        F.round(F.col("burst_id") / F.nullif(F.col("session_kwh"), F.lit(0)), 3).alias("burst_id_per_session_kwh"),
    )
    .orderBy(F.col("burst_id_per_session_kwh").desc(), F.col("session_kwh").desc())
)
