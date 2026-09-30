from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_by_agent_hired_on_w = Window.partitionBy("event_id").orderBy(F.col("agent_hired_on_week").desc(), "id", "event_id")
w_event_type = Window.partitionBy("event_type")
w_event_id_by_agent_hired_on_w_2 = Window.partitionBy("event_id").orderBy(F.col("agent_hired_on_week").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_id_by_agent_hired_on_w_3 = Window.partitionBy("event_id").orderBy(F.col("agent_hired_on_week").desc(), "id", "event_id").rowsBetween(-2, Window.currentRow)
w_event_id_by_event_type_max_r = Window.partitionBy("event_id").orderBy("event_type_max_requester_id", "id", "event_id")
w_distinct_event_actor_in_even = Window.partitionBy("distinct_event_actor_in_event_type")

cleaned_tickets = df_tickets.filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date"))

joined = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    joined
    .filter(F.col("requester_id").between(7, 25))
    .select(
        "*",
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
        F.date_trunc("week", F.col("agent_hired_on")).alias("agent_hired_on_week"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

with_children_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "escalated"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.priority",
        "s.agent_hired_on_week",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

with_windows = (
    with_children_events
    .select(
        "id",
        "priority",
        "agent_hired_on_week",
        "event_id",
        F.rank().over(w_event_id_by_agent_hired_on_w).alias("agent_hired_on_week_rank"),
        F.size(F.collect_set("event_actor").over(w_event_type)).alias("distinct_event_actor_in_event_type"),
        F.max("requester_id").over(w_event_id_by_agent_hired_on_w_2).alias("max_requester_id_so_far"),
        F.sum("requester_id").over(w_event_id_by_agent_hired_on_w_3).alias("requester_id_sum_3"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_event_type), F.lit(0)), 4).alias("requester_id_share_of_event_type"),
        F.max("requester_id").over(w_event_type).alias("event_type_max_requester_id"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.lag("requester_id_sum_3", 2).over(w_event_id_by_event_type_max_r).alias("prev2_requester_id_sum_3"),
        F.size(F.collect_set("event_id").over(w_distinct_event_actor_in_even)).alias("distinct_event_id_in_distinct_event_actor_in_eve"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "priority",
        "agent_hired_on_week",
        "agent_hired_on_week_rank",
        "distinct_event_actor_in_event_type",
        "max_requester_id_so_far",
        "requester_id_sum_3",
        "requester_id_share_of_event_type",
        "distinct_event_id_in_distinct_event_actor_in_eve",
    )
)
