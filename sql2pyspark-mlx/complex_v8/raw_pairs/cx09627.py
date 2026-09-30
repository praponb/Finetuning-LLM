from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("event_type") == "resolved") & (F.col("event_at") < F.lit("2025-02-12 08:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    prepared
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.initcap(F.trim(F.col("actor"))).alias("actor_clean"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(~F.col("actor").isin("customer", "system"), 1)).alias("event_actor_count"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_clean",
        "event_type_prefix",
        "event_actor_count",
        "event_count",
        "event_count_2",
    )
    .orderBy("ticket_id")
)
