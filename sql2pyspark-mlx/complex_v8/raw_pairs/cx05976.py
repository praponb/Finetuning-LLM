from pyspark.sql import functions as F

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

rollup_base = (
    covered
    .groupBy(F.year("event_at").alias("event_at_year"), "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
)

result = (
    rollup_base
    .cube("row_count", "event_at_year")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("row_count", 0.5).alias("median_row_count"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("event_at_year").asc_nulls_last(),
    )
)
