from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_type_len_per_ev = Window.orderBy(F.col("event_type_len_per_event_type_len_2").desc(), "id")
w_all_by_event_type_len_per_ev_2 = Window.orderBy(F.col("event_type_len_per_event_type_len_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_len_2_event_type = Window.partitionBy("event_type_len_2", "event_type")

base_events = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(8, 12)) | (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len_2"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

summarized_rows = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at")
)

tagged = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len_2",
        "first_event_at",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.round(F.col("event_type_len") / F.nullif(F.col("event_type_len_2"), F.lit(0)), 3).alias("event_type_len_per_event_type_len_2"),
    )
)

event_type_stats = (
    tagged
    .groupBy("event_type")
    .agg(F.max("event_type_len_2").alias("max_event_type_len_2"))
)

vs_group = (
    tagged.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_type_len_2") == F.col("g.max_event_type_len_2"))
    .select(
        "s.*",
        "g.max_event_type_len_2",
        F.round(F.col("s.event_type_len_2") - F.col("g.max_event_type_len_2"), 2).alias("event_type_len_2_minus_max"),
    )
)

with_trends = (
    vs_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len_2",
        "first_event_at",
        "event_type_len_per_event_type_len_2",
        "max_event_type_len_2",
        "event_type_len_2_minus_max",
        F.round(F.percent_rank().over(w_all_by_event_type_len_per_ev), 3).alias("event_type_len_per_event_type_len_2_percent_rank"),
        F.nth_value("actor_len", 2).over(w_all_by_event_type_len_per_ev_2).alias("second_actor_len"),
        F.round(F.col("actor_len") - F.avg("actor_len").over(w_event_type_len_2_event_type), 2).alias("actor_len_vs_event_type_len_2_event_type_avg"),
    )
)

joined_ticket = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.first_event_at",
        "s.event_type_len_per_event_type_len_2",
        "s.max_event_type_len_2",
        "s.event_type_len_2_minus_max",
        "s.event_type_len_per_event_type_len_2_percent_rank",
        "s.second_actor_len",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    joined_ticket
    .select(
        "id",
        "ticket_id",
        "first_event_at",
        "event_type_len_per_event_type_len_2",
        "max_event_type_len_2",
        "event_type_len_2_minus_max",
        "ticket_closed_at",
        "agent_hired_on",
        F.add_months(F.col("agent_hired_on"), 6).alias("agent_hired_on_plus_6m"),
        F.when(F.col("agent_tier").isin("T2", "T3"), "group_a").when(F.col("agent_tier") == "T1", "group_b").otherwise("other").alias("agent_tier_group"),
    )
    .orderBy(F.col("agent_tier_group").desc(), F.col("agent_hired_on").desc())
)
