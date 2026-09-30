from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.when(F.col("event_type").isin("assigned", "escalated"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
    )
)

summary = (
    cleaned_events
    .groupBy("event_type", "actor")
    .agg(
        F.sum(F.when(F.col("event_type") == "escalated", 1).otherwise(0)).alias("n_event_type_escalated"),
        F.round(100 * F.count(F.when(F.col("event_type") != "reopened", 1)) / F.count("*"), 1).alias("pct_event_type_reopened"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
)

tagged = (
    summary
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.abs(F.col("n_event_type_escalated") - 6).alias("n_event_type_escalated_distance"),
    )
)

tagged_2 = (
    tagged
    .filter(F.col("pct_event_type_reopened") < 90.0)
    .select(
        "event_type",
        "actor",
        "n_event_type_escalated_distance",
        F.round(F.col("pct_event_type_reopened") * 1.1, 2).alias("pct_event_type_reopened_adj"),
    )
)

result = (
    tagged_2
    .select(
        "event_type",
        "actor",
        "n_event_type_escalated_distance",
        "pct_event_type_reopened_adj",
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
    .orderBy(
        F.col("pct_event_type_reopened_adj").asc_nulls_last(),
        F.col("n_event_type_escalated_distance").desc(),
        "event_type",
        "actor",
    )
    .limit(5)
)
