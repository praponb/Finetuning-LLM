from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id", "agent_id").orderBy("hours_since_open", "id")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

deduped = (
    covered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupingSets(
        [["event_type", "agent_id", "priority"], ["event_type"]],
        "event_type", "agent_id", "priority",
    )
    .agg(
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.sum(F.when(F.col("priority").isin("P4", "P1", "P2"), F.col("hours_since_open"))).alias("hours_since_open_priority_in_p4_p1_p2"),
        F.round(100 * F.count(F.when(F.col("event_type").isNotNull(), 1)) / F.count("*"), 1).alias("pct_event_type_is_not_null"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "agent_id",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "min_hours_since_open",
        "hours_since_open_priority_in_p4_p1_p2",
        "pct_event_type_is_not_null",
    )
)
