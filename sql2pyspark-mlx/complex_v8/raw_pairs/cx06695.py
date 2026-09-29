from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

tagged = (
    prepared
    .filter(~F.col("event_type").isin("created", "resolved"))
    .select(
        "*",
        F.when(F.col("event_type").isin("replied", "resolved"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
        F.when(F.col("event_type").isin("created", "resolved"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group_2"),
    )
)

result = (
    tagged
    .groupingSets(
        [["actor", "actor_or_unknown", "ticket_id"], ["actor"], ["actor_or_unknown"], []],
        "actor", "actor_or_unknown", "ticket_id",
    )
    .agg(
        F.count(F.when(F.col("actor") != "customer", 1)).alias("n_actor_customer"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("actor_or_unknown").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
