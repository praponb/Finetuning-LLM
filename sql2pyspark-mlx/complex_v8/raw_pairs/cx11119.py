from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_event_type = Window.partitionBy("actor", "event_type")
w_ticket_id_by_agent_hired_on = Window.partitionBy("ticket_id").orderBy(F.col("agent_hired_on").desc(), "id")

valid_events = (
    df_ticket_events
    .filter(F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

joined = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.agent_id") >= 4)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_actor_label",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

screened = (
    joined.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.ticket_agent_id"), "left_semi")
    .filter(F.col("ticket_priority") == "P3")
)

joined_agent = (
    screened.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.ticket_agent_id")) & (F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-03-30").cast("date"))), "left")
    .select("s.*", F.col("su.hired_on").alias("agent_hired_on"))
)

windowed = (
    joined_agent
    .select(
        "*",
        F.size(F.collect_set("ticket_agent_id").over(w_actor_event_type)).alias("distinct_ticket_agent_id_in_actor_event_type"),
        F.rank().over(w_ticket_id_by_agent_hired_on).alias("agent_hired_on_rank"),
        F.lead("ticket_csat", 2).over(w_ticket_id_by_agent_hired_on).alias("next2_ticket_csat"),
    )
)

rollup_base = (
    windowed
    .filter(F.col("ticket_csat").between(1, 5))
    .groupBy("agent_hired_on_rank", "next2_ticket_csat")
    .agg(
        F.round(F.avg("agent_hired_on_rank"), 2).alias("avg_agent_hired_on_rank"),
        F.min("event_at").alias("first_event_at"),
        F.max("agent_hired_on_rank").alias("max_agent_hired_on_rank_check"),
    )
    .filter(F.col("max_agent_hired_on_rank_check") > 5)
    .drop("max_agent_hired_on_rank_check")
)

screened_2 = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("agent_hired_on_rank").alias("overall_avg_agent_hired_on_rank")))
    .filter(F.col("agent_hired_on_rank") > F.col("overall_avg_agent_hired_on_rank"))
    .drop("overall_avg_agent_hired_on_rank")
)

result = (
    screened_2
    .rollup("next2_ticket_csat", "agent_hired_on_rank")
    .agg(
        F.grouping("next2_ticket_csat").alias("is_total_next2_ticket_csat"),
        F.count("*").alias("cnt"),
        F.max("first_event_at").alias("last_first_event_at"),
    )
    .orderBy(
        F.col("next2_ticket_csat").asc_nulls_last(),
        F.col("agent_hired_on_rank").asc_nulls_last(),
    )
)
