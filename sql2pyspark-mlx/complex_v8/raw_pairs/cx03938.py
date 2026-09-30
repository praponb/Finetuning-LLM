from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

scored = (
    scoped_events
    .select(
        "id",
        "event_type",
        "actor",
        "event_type_slug",
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix"),
    )
)

totals = (
    scored
    .groupBy("event_type", "actor")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count("*").alias("cnt"),
        F.count(F.when(~F.col("actor").isin("system", "agent"), 1)).alias("n_actor_not_in_system_agent"),
        F.sum(F.when(F.col("event_type").isin("created", "replied", "escalated"), 1).otherwise(0)).alias("n_event_type_in_created_replie"),
    )
)

result = (
    totals
    .groupBy("cnt", "n_event_type_in_created_replie")
    .pivot("event_type", ["escalated", "resolved", "created", "replied"])
    .agg(F.count("distinct_event_types"))
    .orderBy("cnt", "n_event_type_in_created_replie")
)
