from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel").orderBy("opened_at", "id")
w_channel_running = Window.partitionBy("channel").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_by_events_ties = Window.partitionBy("events").orderBy("events")
w_events_session_id = Window.partitionBy("events", "session_id")
w_events_by_events = Window.partitionBy("events").orderBy("events", "channel", "session_id")
w_events_by_session_start_runn = Window.partitionBy("events").orderBy("session_start", "channel", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-06").cast("date"))
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
    )
)

with_prev = base.withColumn("prev_opened_at", F.lag("opened_at").over(w_channel))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_channel_running))

sessions = (
    sessionized
    .groupBy("channel", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    sessions
    .select(
        "channel",
        "session_id",
        "session_start",
        "events",
        "session_csat",
        "duration_min",
        F.dense_rank().over(w_events_by_events_ties).alias("events_dense_rank"),
        F.round(F.avg("session_id").over(w_events_session_id), 2).alias("events_session_id_avg_session_id"),
        F.rank().over(w_events_by_events).alias("events_rank"),
        F.max("session_csat").over(w_events_by_session_start_runn).alias("max_session_csat_so_far"),
        F.count(F.lit(1)).over(w_events_session_id).alias("rows_in_events_session_id"),
        F.round(F.col("events") / F.nullif(F.sum("events").over(w_events_session_id), F.lit(0)), 4).alias("events_share_of_events_session_id"),
    )
)

derived = (
    ranked
    .filter(F.col("channel").isin("web", "chat", "phone"))
    .select(
        "channel",
        "session_id",
        "duration_min",
        "events_dense_rank",
        "events_share_of_events_session_id",
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
    )
)

result = (
    derived
    .filter(F.col("events_dense_rank") <= 8)
    .select(
        "channel",
        "session_id",
        "duration_min",
        "events_dense_rank",
        "events_share_of_events_session_id",
        "session_id_filled",
    )
    .orderBy(
        F.col("channel").desc(),
        F.col("duration_min").desc_nulls_first(),
        "session_id",
    )
    .limit(10)
)
