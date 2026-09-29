from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group = Window.partitionBy("event_type_group")
w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy("ticket_requester_id", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_ticket_events
    .filter((F.col("event_type") != "assigned") | (F.col("event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.when(F.col("event_type").isin("resolved", "created"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
    )
)

joined_ticket = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

with_trends = (
    joined_ticket
    .select(
        "*",
        F.max("ticket_requester_id").over(w_event_type_group).alias("event_type_group_max_ticket_requester_id"),
        F.size(F.collect_set("actor").over(w_event_type_group)).alias("distinct_actor_in_event_type_group"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_event_type_group), 2).alias("ticket_requester_id_vs_event_type_group_avg"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_id_by_ticket_requeste), 2).alias("ticket_requester_id_avg_4"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_at_label",
        "s.actor_group",
        "s.ticket_channel",
        "s.ticket_opened_at",
        "s.event_type_group_max_ticket_requester_id",
        "s.distinct_actor_in_event_type_group",
        "s.ticket_requester_id_vs_event_type_group_avg",
        "s.ticket_requester_id_avg_4",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    combined_details
    .select(
        "id",
        F.col("ticket_requester_id_vs_event_type_group_avg").alias("ticket_requester_id_vs_event_type_group_avg_v"),
        F.col("ticket_requester_id_avg_4").alias("ticket_requester_id_avg_4_v"),
    )
    .unpivot(
        ids=["id"],
        values=["ticket_requester_id_vs_event_type_group_avg_v", "ticket_requester_id_avg_4_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)
