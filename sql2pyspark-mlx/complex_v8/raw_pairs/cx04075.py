from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor")

valid_events = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("escalated", "created"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

with_windows = (
    valid_events
    .withColumn("rows_in_actor", F.count(F.lit(1)).over(w_actor))
    .withColumn("days_since_event_at_vs_actor_avg", F.round(F.col("days_since_event_at") - F.avg("days_since_event_at").over(w_actor), 2))
)

enriched = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "days_since_event_at",
        "actor_clean",
        "rows_in_actor",
        "days_since_event_at_vs_actor_avg",
        F.substring(F.trim(F.col("actor_clean")), 1, 3).alias("actor_clean_prefix"),
    )
)

enriched_2 = (
    enriched
    .filter(F.col("days_since_event_at").between(175, 455))
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_group",
        "days_since_event_at",
        "actor_clean",
        "rows_in_actor",
        "days_since_event_at_vs_actor_avg",
        "actor_clean_prefix",
        F.coalesce(F.col("event_type_group"), F.lit("UNKNOWN")).alias("event_type_group_or_unknown"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        F.col("rows_in_actor").alias("m_rows_in_actor"),
        F.col("days_since_event_at").alias("days_since_event_at_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_rows_in_actor", "days_since_event_at_v"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
