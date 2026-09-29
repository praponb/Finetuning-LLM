from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("actor").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

grouped = (
    valid_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "replied", 1).otherwise(0)).alias("n_event_type_replied"),
        F.sum(F.when(F.col("event_type").isin("resolved", "created"), 1).otherwise(0)).alias("n_event_type_in_resolved_creat"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("n_actor_system"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.max("n_actor_system").alias("overall_max_n_actor_system")))
    .filter(F.col("n_actor_system") < F.col("overall_max_n_actor_system"))
    .drop("overall_max_n_actor_system")
)

scored = (
    filtered
    .filter(F.col("n_actor_system").between(1, 7))
    .select(
        "event_type",
        "n_event_type_replied",
        "n_event_type_in_resolved_creat",
        "n_actor_system",
        F.when(F.col("n_actor_system") >= 6, "A").when(F.col("n_actor_system") >= 2, "B").otherwise("C").alias("n_actor_system_tier"),
    )
)

result = (
    scored
    .select(
        "event_type",
        "n_event_type_in_resolved_creat",
        "n_actor_system",
        "n_event_type_replied",
    )
    .unpivot(
        ids=["event_type"],
        values=["n_event_type_in_resolved_creat", "n_actor_system", "n_event_type_replied"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("event_type", "metric")
)
