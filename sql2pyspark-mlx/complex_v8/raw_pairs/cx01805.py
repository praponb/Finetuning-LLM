from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    prepared
    .filter(F.col("actor") != "customer")
    .select(
        "*",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
    )
)

annotated = (
    metrics
    .select(
        "*",
        F.abs(F.col("event_at_cume_dist") - 0.5).alias("event_at_cume_dist_distance"),
    )
)

result = (
    annotated
    .filter(F.col("actor_or_unknown") != "UNKNOWN")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_at_hour",
        "actor_or_unknown",
        "actor_group",
        "minutes_since_prev_event_at",
    )
    .orderBy(F.col("ticket_id").desc())
)
