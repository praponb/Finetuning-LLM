from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_ticket_count_by_age = Window.partitionBy("priority", "ticket_count").orderBy(F.col("agent_hired_on").desc_nulls_first(), "id", "tag_pos")
w_priority_ticket_count_by_age_2 = Window.partitionBy("priority", "ticket_count").orderBy(F.col("agent_hired_on").desc_nulls_first())
w_agent_id_by_opened_at_moving = Window.partitionBy("agent_id").orderBy("opened_at", "id", "tag_pos").rowsBetween(-6, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter((F.size(F.col("tags")) >= 1) | (F.col("agent_id") <= 5))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        F.greatest(F.col("requester_id"), F.col("csat")).alias("greatest_requester_id_csat"),
    )
)

exploded = (
    scoped_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "greatest_requester_id_csat",
        F.posexplode("tags").alias("tag_pos", "tag"),
    )
)

with_parent = (
    exploded.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.greatest_requester_id_csat",
        "s.tag_pos",
        "s.tag",
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type").isin("replied", "assigned"), 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

agent_id_stats = (
    with_stats
    .groupBy("agent_id")
    .agg(F.max("tag_pos").alias("max_tag_pos"))
)

above_average = (
    with_stats.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.tag_pos") == F.col("g.max_tag_pos"))
    .select(
        "s.*",
        "g.max_tag_pos",
        F.round(F.col("s.tag_pos") - F.col("g.max_tag_pos"), 2).alias("tag_pos_minus_max"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats_2 = (
    above_average.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_closed_at",
    )
)

with_trends = (
    with_stats_2
    .select(
        "*",
        F.dense_rank().over(w_priority_ticket_count_by_age).alias("agent_hired_on_dense_rank"),
        F.round(F.percent_rank().over(w_priority_ticket_count_by_age_2), 3).alias("agent_hired_on_percent_rank"),
        F.round(F.avg("tag_pos").over(w_agent_id_by_opened_at_moving), 2).alias("tag_pos_avg_7"),
        F.rank().over(w_priority_ticket_count_by_age_2).alias("agent_hired_on_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "greatest_requester_id_csat",
        "tag_pos",
        "tag",
        "agent_name",
        "event_event_type_count",
        "event_count",
        "max_tag_pos",
        "tag_pos_minus_max",
        "ticket_count",
        "agent_hired_on_dense_rank",
        "agent_hired_on_percent_rank",
        "tag_pos_avg_7",
        "agent_hired_on_rank",
        F.round((F.unix_timestamp(F.col("last_ticket_closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_last_ticket_closed_at"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
    .orderBy(F.col("opened_at").desc())
)
