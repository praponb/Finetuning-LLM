from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("actor") == "agent") | (F.month(F.col("event_at")).isin(2, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

tagged = (
    scoped_events
    .filter(F.col("event_at_label").isNotNull())
    .select(
        "*",
        F.when(F.col("event_type").isin("reopened", "resolved"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.concat(F.col("event_at_label"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_at_label_ref"),
    )
)

result = (
    tagged
    .rollup("event_type_group", "event_type")
    .agg(
        F.grouping("event_type_group").alias("is_total_event_type_group"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(~F.col("event_type").isin("resolved", "escalated"), 1)).alias("n_event_type_not_in_resolved_e"),
    )
)
