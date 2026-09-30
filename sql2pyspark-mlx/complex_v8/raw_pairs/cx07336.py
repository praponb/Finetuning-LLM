from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id = Window.partitionBy("session_id")
w_site_id_n_events_by_session = Window.partitionBy("site_id", "n_events").orderBy(F.col("session_end").desc(), "site_id", "session_id")
w_session_id_n_events_by_sessi = Window.partitionBy("session_id", "n_events").orderBy(F.col("session_end").desc(), "site_id", "session_id").rowsBetween(-1, Window.currentRow)

valid = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_site_id_running))

sessions = (
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

with_trends = (
    sessions
    .select(
        "*",
        F.round(F.col("session_id") - F.avg("session_id").over(w_session_id), 2).alias("session_id_vs_session_id_avg"),
        F.rank().over(w_site_id_n_events_by_session).alias("session_end_rank"),
        F.max("n_events").over(w_session_id_n_events_by_sessi).alias("n_events_max_2"),
    )
)

result = (
    with_trends
    .select(
        "site_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        "session_end_rank",
    )
    .orderBy(F.col("site_id").desc(), "session_id")
    .limit(20)
)
