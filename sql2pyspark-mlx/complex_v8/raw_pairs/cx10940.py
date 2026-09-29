from pyspark.sql import functions as F

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.requester_id").alias("prior_requester_id_3d"),
    )
)

agent_id_stats = (
    matched
    .groupBy("agent_id")
    .agg(F.avg("prior_rows_3d").alias("avg_prior_rows_3d"))
)

outliers = (
    matched.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_rows_3d") > F.col("g.avg_prior_rows_3d"))
    .select(
        "s.*",
        "g.avg_prior_rows_3d",
        F.round(F.col("s.prior_rows_3d") - F.col("g.avg_prior_rows_3d"), 2).alias("prior_rows_3d_minus_avg"),
    )
)

line_level = (
    outliers.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_3d",
        "s.prior_requester_id_3d",
        "s.prior_rows_3d_minus_avg",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

joined_event = (
    line_level.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_3d_minus_avg",
        "s.event_id",
        "s.event_type",
        "s.event_actor",
        "s.event_at",
        F.col("t.ticket_id").alias("event_ticket_id"),
    )
)

result = (
    joined_event
    .select(
        "id",
        "opened_at",
        "prior_rows_3d_minus_avg",
        "event_type",
        "event_actor",
        "event_at",
        "event_ticket_id",
        F.lower(F.regexp_replace(F.trim(F.col("event_actor")), " ", "_")).alias("event_actor_slug"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(F.col("event_type_or_unknown").asc_nulls_last())
)
