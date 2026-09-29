from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(2, 3))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

rollup_base = (
    filtered
    .groupBy("event_type", "actor")
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum(F.when(F.col("event_type") == "escalated", 1).otherwise(0)).alias("n_event_type_escalated"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

tagged = (
    rollup_base
    .withColumn("event_type_ref", F.concat(F.col("event_type"), F.lit("#"), F.col("n_event_type_escalated").cast("string")))
)

exploded = (
    tagged
    .select(
        "event_type",
        "actor",
        "n_event_type_escalated",
        "distinct_event_types",
        "event_type_ref",
        F.explode("actor_list").alias("actor_list_item"),
    )
)

derived = (
    exploded
    .filter(F.col("actor").isin("system", "customer"))
    .select(
        "event_type",
        "actor",
        "event_type_ref",
        "actor_list_item",
        F.round(F.col("n_event_type_escalated") / F.nullif(F.col("distinct_event_types"), F.lit(0)), 3).alias("n_event_type_escalated_per_distinct_event_types"),
        F.when(F.col("event_type").isin("replied", "escalated"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

derived_2 = (
    derived
    .select(
        "*",
        F.when(F.col("actor").isin("system", "customer"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

derived_3 = (
    derived_2
    .filter(F.col("event_type_group").isNotNull())
    .select(
        "event_type",
        "actor",
        "actor_list_item",
        "n_event_type_escalated_per_distinct_event_types",
        "actor_group",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

result = (
    derived_3
    .select(
        "event_type",
        "actor",
        "n_event_type_escalated_per_distinct_event_types",
        "actor_group",
        "event_type_or_unknown",
    )
    .orderBy(F.col("actor_group").desc_nulls_first(), "actor")
)
