from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor") != "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count_2"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    with_stats
    .cube("actor", "event_type", "ticket_id")
    .agg(
        F.grouping("actor").alias("is_total_actor"),
        F.sum(F.when(F.col("event_type") != "reopened", 1).otherwise(0)).alias("n_event_type_reopened"),
        F.sum("event_count").alias("total_event_count"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("n_event_type_reopened_2"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
