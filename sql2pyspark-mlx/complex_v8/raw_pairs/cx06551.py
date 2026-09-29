from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")

valid = (
    df_ticket_events
    .filter(F.col("event_type").isin("resolved", "replied", "reopened"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

with_counts = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

sequenced = (
    with_counts
    .select(
        "id",
        "actor_or_unknown",
        "last_event_at",
        "event_count",
        F.nth_value("event_at_plus_1d", 2).over(w_all_by_event_at_full).alias("second_event_at_plus_1d"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "actor_or_unknown",
        "last_event_at",
        "second_event_at_plus_1d",
    )
    .orderBy(F.col("id").desc())
)
