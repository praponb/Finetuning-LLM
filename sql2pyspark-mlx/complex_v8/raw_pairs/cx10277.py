from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_days = Window.partitionBy("ticket_id", "event_type").orderBy("days_since_event_at", "id")
w_ticket_id_event_type_by_days_2 = Window.partitionBy("ticket_id", "event_type").orderBy("days_since_event_at", "id").rowsBetween(-3, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        (F.col("event_at") + F.expr("INTERVAL 15 MINUTES")).alias("event_at_plus_15m"),
    )
)

qualified = (
    cleaned_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

with_activity = (
    qualified.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(~F.col("actor").isin("agent", "customer"))
)

event_type_stats = (
    with_activity
    .groupBy("event_type")
    .agg(F.percentile_approx("days_since_event_at", 0.5).alias("median_days_since_event_at"))
)

outliers = (
    with_activity.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.days_since_event_at") >= F.col("g.median_days_since_event_at"))
    .select(
        "s.*",
        "g.median_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.median_days_since_event_at"), 2).alias("days_since_event_at_minus_median"),
    )
)

with_trends = (
    outliers
    .select(
        "*",
        F.ntile(3).over(w_ticket_id_event_type_by_days).alias("days_since_event_at_tercile"),
        F.rank().over(w_ticket_id_event_type_by_days).alias("days_since_event_at_rank"),
        F.round(F.avg("median_days_since_event_at").over(w_ticket_id_event_type_by_days_2), 2).alias("median_days_since_event_at_avg_4"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "days_since_event_at",
        "event_type_slug",
        "event_at_plus_15m",
        "median_days_since_event_at",
        "days_since_event_at_rank",
        "median_days_since_event_at_avg_4",
    )
    .orderBy(F.col("days_since_event_at").desc(), F.col("actor").desc())
)
