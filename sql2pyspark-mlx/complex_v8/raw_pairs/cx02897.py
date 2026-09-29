from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_session_id_by_session_csat = Window.partitionBy("session_id").orderBy(F.col("session_csat").asc_nulls_last(), "priority", "session_id")
w_session_id_by_session_csat_m = Window.partitionBy("session_id").orderBy(F.col("session_csat").asc_nulls_last(), "priority", "session_id").rowsBetween(-2, Window.currentRow)
w_session_id_by_session_csat_f = Window.partitionBy("session_id").orderBy(F.col("session_csat").asc_nulls_last(), "priority", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tickets = (
    df_tickets
    .filter(~F.col("priority").isin("P2", "P4"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    sessions
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        F.ntile(3).over(w_session_id_by_session_csat).alias("session_csat_tercile"),
        F.round(F.cume_dist().over(w_session_id_by_session_csat), 3).alias("session_csat_cume_dist"),
        F.round(F.avg("session_csat").over(w_session_id_by_session_csat_m), 2).alias("session_csat_avg_3"),
        F.last("duration_min").over(w_session_id_by_session_csat_f).alias("last_duration_min"),
        F.nth_value("session_id", 2).over(w_session_id_by_session_csat_f).alias("second_session_id"),
    )
)

result = (
    sequenced
    .groupBy("session_csat_tercile")
    .pivot("priority", ["P4", "P2", "P3", "P1"])
    .agg(F.avg("session_csat_avg_3"))
    .orderBy("session_csat_tercile")
)
