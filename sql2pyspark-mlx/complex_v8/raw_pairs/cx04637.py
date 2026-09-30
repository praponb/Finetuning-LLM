from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")), "left_semi")
    .filter(~F.col("actor").isin("customer", "system"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") != "replied", 1)).alias("event_event_type_count"),
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
    )
)

rollup_base = (
    with_counts
    .groupBy("event_type", F.year("event_at").alias("event_at_year"), "event_count")
    .agg(
        F.max("event_event_type_count").alias("max_event_event_type_count"),
        F.max("first_event_at").alias("last_first_event_at"),
        F.round(F.stddev("event_event_type_count"), 2).alias("stddev_event_event_type_count"),
        F.min("first_event_at").alias("first_first_event_at"),
        F.count("*").alias("row_count"),
    )
)

derived = (
    rollup_base
    .select(
        "event_type",
        "event_at_year",
        "event_count",
        "max_event_event_type_count",
        "last_first_event_at",
        "stddev_event_event_type_count",
        "row_count",
        F.when(F.col("event_type").isin("escalated", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

annotated = (
    derived
    .filter(F.col("event_count") < 5)
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_group")), " ", "_")).alias("event_type_group_slug"),
        F.abs(F.col("row_count") - 5).alias("row_count_distance"),
    )
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.min("event_at_year").alias("overall_min_event_at_year")))
    .filter(F.col("event_at_year") > F.col("overall_min_event_at_year"))
    .drop("overall_min_event_at_year")
)

result = (
    with_activity
    .select(
        "event_type",
        "event_at_year",
        "event_count",
        "max_event_event_type_count",
        "last_first_event_at",
        "stddev_event_event_type_count",
        "row_count",
        "event_type_group",
        "event_type_group_slug",
        "row_count_distance",
    )
    .orderBy(F.col("event_type_group_slug").desc())
)
