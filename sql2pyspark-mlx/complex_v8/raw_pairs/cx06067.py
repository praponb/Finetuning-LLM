from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_prior_6 = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, -1)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id = Window.partitionBy("ticket_id")

cleaned_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(2, 3))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_windows = (
    cleaned_events
    .select(
        "*",
        F.sum("days_since_event_at").over(w_actor_by_event_at_prior_6).alias("days_since_event_at_sum_6_prior"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
        F.round(F.col("days_since_event_at") / F.nullif(F.sum("days_since_event_at").over(w_ticket_id), F.lit(0)), 4).alias("days_since_event_at_share_of_ticket_id"),
    )
)

labeled = (
    with_windows
    .filter(F.col("event_type").isin("escalated", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "days_since_event_at_sum_6_prior",
        "first_event_at",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        (F.col("first_event_at") + F.expr("INTERVAL 30 MINUTES")).alias("first_event_at_plus_30m"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

result = (
    labeled
    .groupingSets(
        [["event_type_or_unknown", "actor_or_unknown", "ticket_id"], ["event_type_or_unknown"]],
        "event_type_or_unknown", "actor_or_unknown", "ticket_id",
    )
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.round(F.avg("days_since_event_at_sum_6_prior"), 2).alias("avg_days_since_event_at_sum_6_prior"),
        F.min("first_event_at_plus_30m").alias("first_first_event_at_plus_30m"),
    )
    .orderBy(
        F.col("event_type_or_unknown").asc_nulls_last(),
        F.col("actor_or_unknown").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
