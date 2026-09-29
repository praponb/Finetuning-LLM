from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor_group", "actor").orderBy("ticket_requester_id_minus_max", "id")
w_dedup_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("max_ticket_requester_id").desc(), F.col("id").desc())

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") == "web"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_group",
        "s.event_at_plus_1d",
        F.col("t.requester_id").alias("ticket_requester_id"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

event_type_stats = (
    with_parent
    .groupBy("event_type")
    .agg(F.max("ticket_requester_id").alias("max_ticket_requester_id"))
)

compared = (
    with_parent.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.ticket_requester_id") == F.col("g.max_ticket_requester_id"))
    .select(
        "s.*",
        "g.max_ticket_requester_id",
        F.round(F.col("s.ticket_requester_id") - F.col("g.max_ticket_requester_id"), 2).alias("ticket_requester_id_minus_max"),
    )
)

latest = (
    compared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("event_actor_count"),
    )
)

summarized_rows = (
    latest.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.last_event_at",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

unique_rows = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    unique_rows
    .filter(F.col("ticket_requester_id") < 22)
    .groupBy(F.date_trunc("month", "first_event_at").alias("first_event_at_month"))
    .agg(
        F.count(F.when(F.col("event_type") != "resolved", 1)).alias("n_event_type_resolved"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
        F.sum(F.when(F.upper(F.trim(F.col("agent_name"))).like("J%"), F.col("event_actor_count"))).alias("event_actor_count_upper_trim_agent_name_like_j"),
    )
)

result = (
    stats
    .filter(F.col("n_event_type_resolved") <= 7)
    .select(
        "first_event_at_month",
        "total_ticket_requester_id",
        "event_actor_count_upper_trim_agent_name_like_j",
    )
    .orderBy(F.col("total_ticket_requester_id").asc_nulls_last())
)
