from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("assigned", "reopened")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.month(F.col("event_at")).alias("event_at_month"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
        F.count("*").alias("event_count_2"),
    )
)

with_counts = (
    base_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

tagged = (
    with_counts
    .filter(F.col("event_at_label").isNotNull())
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "event_at_month",
        "event_count",
        "first_event_at",
        "event_count_2",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

without_matches = (
    tagged
    .crossJoin(tagged.agg(F.avg("event_count_2").alias("overall_avg_event_count_2")))
    .filter(F.col("event_count_2") > F.col("overall_avg_event_count_2"))
    .drop("overall_avg_event_count_2")
)

labeled = (
    without_matches
    .filter(F.col("event_at_month") <= 10)
    .select(
        "*",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

result = (
    labeled
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_label",
        "event_at_month",
        "event_count",
        "first_event_at",
        "event_type_or_unknown",
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label_2"),
    )
    .orderBy(F.col("event_at_month").desc(), "id")
    .limit(20)
)
