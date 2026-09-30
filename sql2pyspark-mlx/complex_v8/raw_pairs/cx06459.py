from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_event_type_by_opened_a = Window.partitionBy("event_event_type").orderBy(F.col("opened_at").asc_nulls_last(), "id", "event_id", "event_id_2").rowsBetween(-2, -1)
w_event_event_type_by_opened_a_2 = Window.partitionBy("event_event_type").orderBy(F.col("opened_at").asc_nulls_last(), "id", "event_id", "event_id_2")

filtered_tickets = (
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
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

exploded_detail = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("channel") != "phone", 1)).alias("ticket_channel_count"),
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

summarized_rows = (
    exploded_detail.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_channel_count"), F.lit(0)).alias("ticket_channel_count"),
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

detail_rows_events = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.tags",
        "s.event_id",
        "s.event_at",
        "s.event_type",
        "s.ticket_channel_count",
        "s.last_ticket_closed_at",
        F.col("t.id").alias("event_id_2"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor_2"),
    )
)

metrics = (
    detail_rows_events
    .select(
        "*",
        F.sum("ticket_channel_count").over(w_event_event_type_by_opened_a).alias("ticket_channel_count_sum_2_prior"),
        F.ntile(5).over(w_event_event_type_by_opened_a_2).alias("opened_at_quintile"),
        F.row_number().over(w_event_event_type_by_opened_a_2).alias("seq_no"),
    )
)

top_per_group = metrics.filter(F.col("seq_no") == 1)

result = (
    top_per_group
    .groupingSets(
        [["event_type", "event_id_2", "event_actor_2"], ["event_type"]],
        "event_type", "event_id_2", "event_actor_2",
    )
    .agg(
        F.round(F.sum("seq_no") / F.nullif(F.sum("ticket_channel_count_sum_2_prior"), F.lit(0)), 3).alias("seq_no_to_ticket_channel_count_sum_2_prior_ratio"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("event_id_2").asc_nulls_last(),
        F.col("event_actor_2").asc_nulls_last(),
    )
)
