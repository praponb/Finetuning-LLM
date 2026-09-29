from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

sessions = (
    sessionized
    .groupBy("site_id", "session_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("kwh_bucket").alias("session_kwh_bucket"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.round(F.col("n_events") / F.nullif(F.col("session_kwh_bucket"), F.lit(0)), 3).alias("n_events_per_session_kwh_bucket"),
        F.greatest(F.col("session_id"), F.col("n_events")).alias("greatest_session_id_n_events"),
    )
    .orderBy(F.col("session_end").desc_nulls_first())
)
