from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id", "agent_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

unique_rows = (
    covered
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    unique_rows.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    enriched
    .cube("agent_team", "ticket_priority", "event_type")
    .agg(
        F.round(100 * F.count(F.when(~F.col("event_type").isin("created", "reopened"), 1)) / F.count("*"), 1).alias("pct_event_type_not_in_created_re"),
        F.count("*").alias("n_rows"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .select(
        F.coalesce(F.col("agent_team"), F.lit("ALL")).alias("agent_team"),
        F.coalesce(F.col("ticket_priority"), F.lit("ALL")).alias("ticket_priority"),
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "pct_event_type_not_in_created_re",
        "n_rows",
        "distinct_prioritys",
    )
    .orderBy(
        F.col("agent_team").asc_nulls_last(),
        F.col("ticket_priority").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
