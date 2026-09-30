from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_event_type = Window.partitionBy("event_event_type")
w_ticket_id_by_event_at_moving = Window.partitionBy("ticket_id").orderBy("event_at", "id", "event_id").rowsBetween(-6, Window.currentRow)
w_event_event_type_ticket_id_b = Window.partitionBy("event_event_type", "ticket_id").orderBy(F.col("days_since_event_at").desc(), "id", "event_id")
w_ticket_id_by_event_at_prior = Window.partitionBy("ticket_id").orderBy("event_at", "id", "event_id").rowsBetween(-3, -1)
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy("event_at", "id", "event_id")

prepared_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

without_matches = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") <= 5) & (F.col("t.closed_at") < F.lit("2025-02-21 07:00:00").cast("timestamp")), "left_semi")
)

exploded_detail = (
    without_matches.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_or_unknown",
        "s.event_at_label",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

event_type_or_unknown_stats = (
    exploded_detail
    .groupBy("event_type_or_unknown")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

outliers = (
    exploded_detail.alias("s")
    .join(event_type_or_unknown_stats.alias("g"), F.col("g.event_type_or_unknown") == F.col("s.event_type_or_unknown"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.round(F.col("median_days_since_event_at") - F.avg("median_days_since_event_at").over(w_event_event_type), 2).alias("median_days_since_event_at_vs_event_event_type_a"),
        F.round(F.avg("days_since_event_at").over(w_ticket_id_by_event_at_moving), 2).alias("days_since_event_at_avg_7"),
        F.row_number().over(w_event_event_type_ticket_id_b).alias("days_since_event_at_seq"),
        F.sum("median_days_since_event_at").over(w_ticket_id_by_event_at_prior).alias("median_days_since_event_at_sum_3_prior"),
        F.round(F.percent_rank().over(w_event_event_type_ticket_id_b), 3).alias("days_since_event_at_percent_rank"),
        F.lag("days_since_event_at", 1, 0).over(w_ticket_id_by_event_at).alias("prev_days_since_event_at"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.days_since_event_at",
        "s.event_type_or_unknown",
        "s.event_at_label",
        "s.event_id",
        "s.event_actor",
        "s.event_event_type",
        "s.median_days_since_event_at",
        "s.days_since_event_at_seq",
        "s.days_since_event_at_percent_rank",
        "s.prev_days_since_event_at",
        F.col("t.event_at").alias("event_event_at_2"),
        F.col("t.actor").alias("event_actor_2"),
    )
)

result = (
    enriched
    .groupBy("event_actor", "event_type_or_unknown")
    .pivot("event_event_type", ["created", "resolved", "replied", "reopened"])
    .agg(F.count("prev_days_since_event_at"))
    .orderBy("event_actor", "event_type_or_unknown")
)
