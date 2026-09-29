from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_group_actor_by_ev = Window.partitionBy("event_type_group", "actor").orderBy(F.col("event_at").desc_nulls_first())
w_dedup = Window.partitionBy("ticket_priority").orderBy(F.col("event_at_rank").desc(), F.col("id").desc())

prepared = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("escalated", "assigned"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

with_trends = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.dense_rank().over(w_event_type_group_actor_by_ev).alias("event_at_dense_rank"),
        F.rank().over(w_event_type_group_actor_by_ev).alias("event_at_rank"),
    )
)

combined_details_ticket = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_at_rank",
        F.col("t.tags").alias("ticket_tags"),
    )
)

enriched = (
    combined_details_ticket.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.tags").alias("ticket_tags_2"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

with_parent = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority").isin("P1", "P2")), "inner")
    .filter(F.col("t.agent_id").isNotNull())
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id_2"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

latest = (
    with_parent
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .rollup("ticket_id", "ticket_priority")
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.max("ticket_requester_id").alias("max_ticket_requester_id"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("ticket_priority").asc_nulls_last(),
    )
)
