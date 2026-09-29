from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy("event_at", "id")

recent = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_at") < F.lit("2025-03-02 15:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

unique_rows = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    unique_rows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_or_unknown",
        F.dayofweek(F.col("event_at_month")).alias("event_at_month_dayofweek"),
        F.substring(F.trim(F.col("actor_or_unknown")), 1, 2).alias("actor_or_unknown_prefix"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

result = (
    tagged
    .groupingSets(
        [["actor_or_unknown", "ticket_id", "event_at_month_dayofweek"], ["actor_or_unknown"], ["ticket_id"]],
        "actor_or_unknown", "ticket_id", "event_at_month_dayofweek",
    )
    .agg(
        F.grouping("actor_or_unknown").alias("is_total_actor_or_unknown"),
        F.round(F.sum("days_since_event_at") / F.nullif(F.sum("event_at_month_dayofweek"), F.lit(0)), 3).alias("days_since_event_at_to_event_at_month_dayofweek_"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .select(
        F.coalesce(F.col("actor_or_unknown"), F.lit("ALL")).alias("actor_or_unknown"),
        "ticket_id",
        "event_at_month_dayofweek",
        "is_total_actor_or_unknown",
        "days_since_event_at_to_event_at_month_dayofweek_",
        "distinct_ticket_ids",
    )
)
