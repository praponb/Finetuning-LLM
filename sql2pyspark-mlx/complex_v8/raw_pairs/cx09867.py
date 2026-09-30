from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_id_by_closed_a = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_id_by_closed_a_2 = Window.partitionBy("channel", "agent_id").orderBy("closed_at", "id")
w_agent_id_by_days_since_close = Window.partitionBy("agent_id").orderBy("days_since_closed_at", "id")
w_channel_agent_id = Window.partitionBy("channel", "agent_id")

valid = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.nth_value("closed_at", 2).over(w_channel_agent_id_by_closed_a).alias("second_closed_at"),
        F.dense_rank().over(w_channel_agent_id_by_closed_a_2).alias("closed_at_dense_rank"),
        F.row_number().over(w_agent_id_by_days_since_close).alias("days_since_closed_at_seq"),
        F.ntile(5).over(w_channel_agent_id_by_closed_a_2).alias("closed_at_quintile"),
        F.sum("days_since_closed_at").over(w_channel_agent_id).alias("channel_agent_id_sum_days_since_closed_at"),
    )
)

line_level_events = (
    with_trends.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-06").cast("date")), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.second_closed_at",
        "s.closed_at_dense_rank",
        "s.channel_agent_id_sum_days_since_closed_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    line_level_events
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "second_closed_at",
        "closed_at_dense_rank",
        "event_id",
        F.least(F.col("requester_id"), F.col("closed_at_dense_rank")).alias("least_requester_id_closed_at_dense_rank"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
    .orderBy("id")
)
