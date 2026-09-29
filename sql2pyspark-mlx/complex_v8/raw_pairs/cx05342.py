from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")

valid = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

joined_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.hired_on").alias("agent_hired_on"),
        "su.agent_name",
    )
)

sequenced = (
    joined_ticket
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "actor_group",
        "ticket_tags",
        "agent_hired_on",
        "agent_name",
        F.lead("agent_hired_on", 3).over(w_all_by_event_at).alias("next3_agent_hired_on"),
        F.lag("agent_hired_on", 2).over(w_all_by_event_at).alias("prev2_agent_hired_on"),
    )
)

result = (
    sequenced
    .filter(F.col("prev2_agent_hired_on") < F.lit("2024-12-31").cast("date"))
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_group",
        "actor_group",
        "ticket_tags",
        "agent_hired_on",
        "agent_name",
        "prev2_agent_hired_on",
        F.substring(F.trim(F.col("event_type_group")), 1, 3).alias("event_type_group_prefix"),
    )
    .orderBy(F.col("agent_hired_on").desc_nulls_first(), "agent_name")
)
