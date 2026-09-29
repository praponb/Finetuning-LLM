from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

annotated = (
    recent
    .filter(F.col("actor") == "agent")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_dayofweek",
        F.abs(F.col("event_at_dayofweek") - 5).alias("event_at_dayofweek_distance"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
)

result = (
    annotated
    .cube("ticket_id", "actor")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("actor").isin("agent", "customer"), 1).otherwise(0)).alias("n_actor_in_agent_customer"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "grouping_level",
        "n_actor_in_agent_customer",
    )
    .orderBy(F.col("ticket_id").asc_nulls_last(), F.col("actor").asc_nulls_last())
)
