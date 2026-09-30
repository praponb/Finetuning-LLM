from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 31) | (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.to_date(F.col("event_at")).alias("event_at_date_2"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
)

grouped = (
    recent
    .groupBy(
        F.year("event_at").alias("event_at_year"),
        "actor",
        F.date_trunc("month", "event_at_date").alias("event_at_date_month"),
    )
    .agg(
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("n_event_type_reopened"),
        F.sum(F.when(F.col("actor").isin("agent", "customer"), 1).otherwise(0)).alias("n_actor_in_agent_customer"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
)

screened = (
    grouped
    .crossJoin(grouped.agg(F.avg("n_event_type_reopened").alias("overall_avg_n_event_type_reopened")))
    .filter(F.col("n_event_type_reopened") > F.col("overall_avg_n_event_type_reopened"))
    .drop("overall_avg_n_event_type_reopened")
)

result = (
    screened
    .select(
        "event_at_year",
        "actor",
        "cnt",
        "distinct_event_types",
        "n_event_type_reopened",
        "distinct_ids",
    )
    .unpivot(
        ids=["event_at_year", "actor"],
        values=["cnt", "distinct_event_types", "n_event_type_reopened", "distinct_ids"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
