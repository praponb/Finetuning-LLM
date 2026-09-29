from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 08:00:00").cast("timestamp")) & (F.col("event_type") != "escalated"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.length(F.trim(F.col("actor"))).alias("actor_len_2"),
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

grouped = (
    base_events
    .groupBy("actor")
    .agg(
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.countDistinct("event_type_or_unknown").alias("distinct_event_type_or_unknowns"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") >= 3)
)

flattened = (
    grouped
    .select(
        "actor",
        "n_event_type_is_not_null",
        "cnt",
        "distinct_event_type_or_unknowns",
        "distinct_ids",
        F.explode("actor_list").alias("actor_list_item"),
    )
)

enriched = (
    flattened
    .filter(F.col("actor") == "system")
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug_2"),
    )
)

labeled = (
    enriched
    .filter(F.col("cnt").between(5, 9))
    .select(
        "actor",
        "n_event_type_is_not_null",
        "cnt",
        "distinct_event_type_or_unknowns",
        "actor_list_item",
        "actor_slug",
        "actor_slug_2",
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
    )
)

qualified = (
    labeled
    .crossJoin(labeled.agg(F.avg("n_event_type_is_not_null").alias("overall_avg_n_event_type_is_not_null")))
    .filter(F.col("n_event_type_is_not_null") > F.col("overall_avg_n_event_type_is_not_null"))
    .drop("overall_avg_n_event_type_is_not_null")
)

result = (
    qualified
    .filter(F.col("distinct_event_type_or_unknowns") >= 5)
    .select(
        "actor",
        "n_event_type_is_not_null",
        "distinct_event_type_or_unknowns",
        "actor_list_item",
        "actor_slug",
        "actor_slug_2",
        "distinct_ids_bucket",
    )
    .orderBy("actor", "actor_slug_2")
)
