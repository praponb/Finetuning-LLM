from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel = Window.partitionBy("ticket_channel")
w_ticket_channel_by_days_since = Window.partitionBy("ticket_channel").orderBy(F.col("days_since_event_at").desc(), "id", "ticket_id_2")
w_ticket_channel_by_days_since_2 = Window.partitionBy("ticket_channel").orderBy(F.col("days_since_event_at").desc(), "id", "ticket_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_events = (
    df_ticket_events
    .filter(F.col("actor") != "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

combined_details_ticket = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

line_level_tickets = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.ticket_agent_id")) & (F.col("t.csat") >= 2), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at_label",
        "s.days_since_event_at",
        "s.ticket_csat",
        "s.ticket_agent_id",
        "s.ticket_channel",
        F.col("t.id").alias("ticket_id_2"),
        F.col("t.csat").alias("ticket_csat_2"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

sequenced = (
    line_level_tickets
    .select(
        "*",
        F.round(F.col("ticket_csat_2") / F.nullif(F.sum("ticket_csat_2").over(w_ticket_channel), F.lit(0)), 4).alias("ticket_csat_2_share_of_ticket_channel"),
        F.lag("days_since_event_at").over(w_ticket_channel_by_days_since).alias("prev_days_since_event_at"),
        F.round(F.col("ticket_csat_2") - F.avg("ticket_csat_2").over(w_ticket_channel), 2).alias("ticket_csat_2_vs_ticket_channel_avg"),
        F.max("ticket_csat").over(w_ticket_channel).alias("ticket_channel_max_ticket_csat"),
        F.nth_value("ticket_opened_at", 2).over(w_ticket_channel_by_days_since_2).alias("second_ticket_opened_at"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["ticket_csat", "ticket_channel", "ticket_priority"], ["ticket_csat"], ["ticket_channel"], []],
        "ticket_csat", "ticket_channel", "ticket_priority",
    )
    .agg(F.grouping_id().alias("grouping_level"), F.count("*").alias("cnt"))
    .orderBy(
        F.col("ticket_csat").asc_nulls_last(),
        F.col("ticket_channel").asc_nulls_last(),
        F.col("ticket_priority").asc_nulls_last(),
    )
)
