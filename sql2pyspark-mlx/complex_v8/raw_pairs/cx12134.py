from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

scored = (
    matched
    .filter(F.col("hours_since_open").between(18.0, 68.4))
    .select(
        "id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
    )
)

summary = (
    scored
    .groupBy("priority")
    .agg(
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.round(F.sum("hours_since_open_bucket") / F.nullif(F.sum("hours_since_open"), F.lit(0)), 3).alias("hours_since_open_bucket_to_hours_since_open_rati"),
        F.min("hours_since_open_bucket").alias("min_hours_since_open_bucket"),
        F.sum("hours_since_open_bucket").alias("total_hours_since_open_bucket"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.max("hours_since_open").alias("max_hours_since_open"),
    )
)

annotated = (
    summary
    .select(
        "*",
        (F.col("max_hours_since_open") + F.col("total_hours_since_open")).alias("max_hours_since_open_plus_total_hours_since_open"),
        F.greatest(F.col("total_hours_since_open"), F.col("max_hours_since_open")).alias("greatest_total_hours_since_open_max_hours_since_"),
        F.abs(F.col("max_hours_since_open") - 25.2).alias("max_hours_since_open_distance"),
    )
)

result = (
    annotated
    .filter(F.col("greatest_total_hours_since_open_max_hours_since_") < 280.8)
    .select(
        "priority",
        "total_hours_since_open",
        "min_hours_since_open_bucket",
        "total_hours_since_open_bucket",
        "priority_list",
        "max_hours_since_open",
        "max_hours_since_open_plus_total_hours_since_open",
        "greatest_total_hours_since_open_max_hours_since_",
        "max_hours_since_open_distance",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
    .orderBy(F.col("max_hours_since_open_distance").desc_nulls_first(), "priority")
    .limit(10)
)
