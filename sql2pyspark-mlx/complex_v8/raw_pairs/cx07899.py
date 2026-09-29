from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id = Window.partitionBy("event_id")
w_agent_id_channel_by_priority = Window.partitionBy("agent_id", "channel").orderBy("priority_len")

scoped = (
    df_tickets
    .filter((~F.array_contains(F.col("tags"), "login")) & (F.col("channel").isin("phone", "chat")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "priority",
        "channel",
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

with_children = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.priority_len",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

screened = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isNotNull()) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-06").cast("date")), "left_semi")
)

sequenced = (
    screened
    .select(
        "*",
        F.round(F.col("priority_len") - F.avg("priority_len").over(w_event_id), 2).alias("priority_len_vs_event_id_avg"),
        F.dense_rank().over(w_agent_id_channel_by_priority).alias("priority_len_dense_rank"),
        F.count(F.lit(1)).over(w_event_id).alias("rows_in_event_id"),
        F.round(F.col("priority_len") / F.nullif(F.sum("priority_len").over(w_event_id), F.lit(0)), 4).alias("priority_len_share_of_event_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "channel",
        "priority_len",
        "event_id",
        "event_actor",
        "event_type",
        "priority_len_dense_rank",
        "priority_len_share_of_event_id",
        F.coalesce(F.col("rows_in_event_id"), F.lit(0)).alias("rows_in_event_id_filled"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
    .orderBy(
        F.col("priority_len_share_of_event_id").asc_nulls_last(),
        "priority_len",
    )
)
