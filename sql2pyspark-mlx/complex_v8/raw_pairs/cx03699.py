from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_hired_on_plus_12m_year = Window.partitionBy(F.year("agent_hired_on_plus_12m"), "ticket_csat").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_agent_hired_on_plus_12m_year_2 = Window.partitionBy(F.year("agent_hired_on_plus_12m"), "ticket_csat").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 33)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

joined = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.csat") < 3)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_type_actor_label",
        "s.actor_prefix",
        "s.event_type_len",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

enriched = (
    joined
    .filter(F.col("agent_hired_on") >= F.lit("2024-07-09").cast("date"))
    .select(
        "*",
        F.greatest(F.col("event_type_len"), F.col("ticket_csat")).alias("greatest_event_type_len_ticket_csat"),
        F.add_months(F.col("agent_hired_on"), 12).alias("agent_hired_on_plus_12m"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_actor_label",
        "actor_prefix",
        "event_type_len",
        "ticket_csat",
        "ticket_opened_at",
        "ticket_agent_id",
        "greatest_event_type_len_ticket_csat",
        "agent_hired_on_plus_12m",
        F.datediff(F.col("agent_hired_on_plus_12m"), F.lag("agent_hired_on_plus_12m").over(w_agent_hired_on_plus_12m_year)).alias("days_since_prev_agent_hired_on_plus_12m"),
        F.first("ticket_opened_at").over(w_agent_hired_on_plus_12m_year_2).alias("first_ticket_opened_at"),
    )
)

result = (
    metrics
    .groupingSets(
        [["ticket_agent_id", "actor", "event_type_len"], ["ticket_agent_id"], ["actor"]],
        "ticket_agent_id", "actor", "event_type_len",
    )
    .agg(
        F.sum("greatest_event_type_len_ticket_csat").alias("total_greatest_event_type_len_ticket_csat"),
    )
    .orderBy(
        F.col("ticket_agent_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("event_type_len").asc_nulls_last(),
    )
)
