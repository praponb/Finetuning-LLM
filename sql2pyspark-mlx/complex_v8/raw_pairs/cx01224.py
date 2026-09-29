from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type")
w_all_by_days_since_opened_at = Window.orderBy(F.col("days_since_opened_at").desc(), "id", "event_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_days_since_ope = Window.partitionBy("event_type").orderBy("days_since_opened_at_minus_avg")
w_last_event_at_year = Window.partitionBy(F.year("last_event_at"))
w_event_actor_by_last_event_at = Window.partitionBy("event_actor").orderBy(F.col("last_event_at").desc(), "id", "event_id").rowsBetween(-1, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("priority").isin("P2", "P4")) & (F.col("csat") <= 4))
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

detail_rows_events = (
    cleaned_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (~F.col("t.event_type").isin("created", "reopened")), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.tags",
        "s.days_since_opened_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

agent_id_stats = (
    detail_rows_events
    .groupBy("agent_id")
    .agg(F.avg("days_since_opened_at").alias("avg_days_since_opened_at"))
)

above_average = (
    detail_rows_events.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.days_since_opened_at") > F.col("g.avg_days_since_opened_at"))
    .select(
        "s.*",
        "g.avg_days_since_opened_at",
        F.round(F.col("s.days_since_opened_at") - F.col("g.avg_days_since_opened_at"), 2).alias("days_since_opened_at_minus_avg"),
    )
)

sequenced = (
    above_average
    .select(
        "id",
        "tags",
        "days_since_opened_at",
        "event_id",
        "event_at",
        "event_type",
        "event_actor",
        "avg_days_since_opened_at",
        "days_since_opened_at_minus_avg",
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.round(F.col("days_since_opened_at") / F.nullif(F.sum("days_since_opened_at").over(w_event_type), F.lit(0)), 4).alias("days_since_opened_at_share_of_event_type"),
        F.last("event_at").over(w_all_by_days_since_opened_at).alias("last_event_at"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.dense_rank().over(w_event_type_by_days_since_ope).alias("days_since_opened_at_minus_avg_dense_rank"),
        F.round(F.col("avg_days_since_opened_at") / F.nullif(F.sum("avg_days_since_opened_at").over(w_last_event_at_year), F.lit(0)), 4).alias("avg_days_since_opened_at_share_of_last_event_at_"),
        F.round(F.avg("avg_days_since_opened_at").over(w_event_actor_by_last_event_at), 2).alias("avg_days_since_opened_at_avg_2"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.when(F.col("days_since_opened_at") >= 490, "HIGH").when(F.col("days_since_opened_at") >= 280, "MEDIUM").otherwise("LOW").alias("days_since_opened_at_tier"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

result = (
    derived
    .filter(F.col("days_since_opened_at_minus_avg_dense_rank") >= 3)
    .select(
        "id",
        "tags",
        "days_since_opened_at",
        "event_id",
        "event_at",
        "event_type",
        "event_actor",
        "avg_days_since_opened_at",
        "days_since_opened_at_minus_avg",
        "rows_in_event_type",
        "days_since_opened_at_share_of_event_type",
        "last_event_at",
        "days_since_opened_at_minus_avg_dense_rank",
        "avg_days_since_opened_at_share_of_last_event_at_",
        "avg_days_since_opened_at_avg_2",
        "days_since_opened_at_tier",
        "event_type_or_unknown",
    )
    .orderBy(F.col("event_actor").desc(), F.col("event_type").desc())
)
