from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
    )
)

with_stats = (
    valid.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

annotated = (
    with_stats
    .filter(F.col("event_type").isin("assigned", "reopened", "resolved"))
    .select(
        "*",
        F.concat_ws(": ", F.col("event_at_label"), F.col("actor")).alias("event_at_label_actor_label"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

tagged = (
    filtered
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_at_label",
        "event_count",
        "event_count_2",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
        F.concat_ws("|", F.col("actor"), F.col("event_at_label")).alias("actor_event_at_label_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_count").cast("string")).alias("event_type_ref"),
    )
)

per_group = (
    tagged
    .groupBy(
        "actor",
        F.year("event_at_quarter").alias("event_at_quarter_year"),
        F.to_date("event_at").alias("event_at_day"),
    )
    .agg(
        F.count(F.when(F.col("actor") != "agent", 1)).alias("n_actor_agent"),
        F.round(100 * F.count(F.when(~F.col("actor").isin("system", "agent"), 1)) / F.count("*"), 1).alias("pct_actor_not_in_system_agent"),
        F.sum(F.when(F.col("event_type") != "created", 1).otherwise(0)).alias("n_event_type_created"),
        F.count("*").alias("row_count"),
    )
)

result = (
    per_group
    .select(
        "actor",
        "n_actor_agent",
        "pct_actor_not_in_system_agent",
        "n_event_type_created",
        "row_count",
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
    .orderBy(
        F.col("pct_actor_not_in_system_agent").desc_nulls_first(),
        F.col("n_event_type_created").asc(),
    )
)
