from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.concat_ws("-", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

totals = (
    valid_events
    .filter(F.col("ticket_id").isNotNull())
    .groupBy("actor", F.date_format("event_at", "yyyy-MM").alias("event_at_ym"))
    .agg(
        F.sum(F.when(F.col("event_type") == "reopened", 1).otherwise(0)).alias("n_event_type_reopened"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("reopened", "escalated", "resolved"), 1)) / F.count("*"), 1).alias("pct_event_type_in_reopened_escal"),
        F.count(F.when(F.col("actor").isin("customer", "agent"), 1)).alias("n_actor_in_customer_agent"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
    )
)

without_matches = (
    totals
    .crossJoin(totals.agg(F.avg("distinct_ids").alias("overall_avg_distinct_ids")))
    .filter(F.col("distinct_ids") > F.col("overall_avg_distinct_ids"))
    .drop("overall_avg_distinct_ids")
)

with_activity = (
    without_matches
    .crossJoin(without_matches.agg(F.max("distinct_ids").alias("overall_max_distinct_ids")))
    .filter(F.col("distinct_ids") < F.col("overall_max_distinct_ids"))
    .drop("overall_max_distinct_ids")
)

exploded = (
    with_activity
    .select(
        "actor",
        "event_at_ym",
        "n_event_type_reopened",
        "distinct_ids",
        "pct_event_type_in_reopened_escal",
        "n_actor_in_customer_agent",
        "distinct_ticket_ids",
        F.posexplode("actor_list").alias("actor_list_item_pos", "actor_list_item"),
    )
)

result = (
    exploded
    .filter(F.col("n_event_type_reopened") > 2)
    .select(
        "actor",
        "distinct_ids",
        "pct_event_type_in_reopened_escal",
        "n_actor_in_customer_agent",
        "distinct_ticket_ids",
        "actor_list_item_pos",
        "actor_list_item",
    )
    .orderBy(F.col("actor_list_item").desc(), F.col("actor").asc_nulls_last())
)
