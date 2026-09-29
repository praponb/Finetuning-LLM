from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
    )
)

agent_id_stats = (
    scoped_tickets
    .groupBy("agent_id")
    .agg(F.max("csat").alias("max_csat"))
)

outliers = (
    scoped_tickets.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") == F.col("g.max_csat"))
    .select(
        "s.*",
        "g.max_csat",
        F.round(F.col("s.csat") - F.col("g.max_csat"), 2).alias("csat_minus_max"),
    )
)

without_matches = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor").isNotNull()), "left_semi")
)

result = (
    without_matches
    .select("id", "max_csat", "csat_minus_max")
    .unpivot(
        ids=["id"],
        values=["max_csat", "csat_minus_max"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
