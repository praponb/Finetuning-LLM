from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_session_start_month = Window.partitionBy("events", F.date_trunc("month", "session_start"))
w_all_by_session_end_running = Window.orderBy(F.col("session_end").desc(), "priority", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_prior_2 = Window.orderBy(F.col("session_end").desc(), "priority", "burst_id").rowsBetween(-2, -1)
w_all_by_session_end_full = Window.orderBy(F.col("session_end").desc(), "priority", "burst_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

with_prev = base_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id_minus_csat").alias("session_requester_id_minus_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    sessions
    .filter(F.col("session_end") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .select(
        "priority",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "session_requester_id_minus_csat",
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
)

sequenced = (
    annotated
    .select(
        "*",
        F.size(F.collect_set("priority").over(w_events_session_start_month)).alias("distinct_priority_in_events_session_start_month"),
        F.count(F.lit(1)).over(w_all_by_session_end_running).alias("running_count"),
        F.max("burst_id").over(w_all_by_session_end_prior_2).alias("burst_id_max_2_prior"),
        F.nth_value("session_start_date", 2).over(w_all_by_session_end_full).alias("second_session_start_date"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["priority", "burst_id"], ["priority"], []],
        "priority", "burst_id",
    )
    .agg(F.count(F.when(F.col("priority") == "P3", 1)).alias("n_priority_p3"))
    .orderBy(F.col("priority").asc_nulls_last(), F.col("burst_id").asc_nulls_last())
)
