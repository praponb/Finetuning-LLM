from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(2, 3)) & (F.col("actor").isin("agent", "system")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("actor").isin("system", "agent"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

summary = (
    filtered_events
    .groupBy("actor")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("n_actor_in_agent_customer"),
        F.max("event_at").alias("last_event_at"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

result = (
    summary
    .select(
        "actor",
        "n_actor_in_agent_customer",
        "distinct_event_types",
        "distinct_actors",
    )
    .unpivot(
        ids=["actor"],
        values=["n_actor_in_agent_customer", "distinct_event_types", "distinct_actors"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
