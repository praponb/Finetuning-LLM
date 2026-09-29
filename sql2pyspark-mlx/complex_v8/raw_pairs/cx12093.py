from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-03-09 04:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "channel",
        "csat",
        F.to_date(F.col("opened_at")).alias("opened_at_date"),
    )
)

annotated = (
    valid_tickets
    .filter(F.col("agent_id") <= 8)
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "opened_at_date",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

rollup_base = (
    annotated
    .groupBy(F.year("opened_at_date").alias("opened_at_date_year"), "channel")
    .agg(
        F.datediff(F.max("opened_at_date"), F.min("opened_at_date")).alias("opened_at_date_span_days"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("days_since_opened_at").alias("total_days_since_opened_at"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("channel").isNotNull(), F.col("requester_id")).otherwise(0)).alias("requester_id_channel_is_not_null"),
    )
)

result = (
    rollup_base
    .groupBy("row_count")
    .pivot("channel", ["web", "phone"])
    .agg(F.sum("opened_at_date_span_days"))
)
