from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 3)) & (F.col("csat").between(2, 5)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 4).alias("priority_prefix"),
    )
)

summary = (
    filtered_tickets
    .groupBy(
        F.to_date("closed_at").alias("closed_at_day"),
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.max("csat").alias("max_csat"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.sum(F.when(F.col("priority").isNotNull(), F.col("csat")).otherwise(0)).alias("csat_priority_is_not_null"),
        F.sum("csat").alias("total_csat"),
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_to_requester_id_ratio"),
    )
)

annotated = (
    summary
    .withColumn("days_since_opened_at_month", F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at_month")))
)

with_activity = (
    annotated
    .crossJoin(annotated.agg(F.avg("csat_to_requester_id_ratio").alias("overall_avg_csat_to_requester_id_ratio")))
    .filter(F.col("csat_to_requester_id_ratio") > F.col("overall_avg_csat_to_requester_id_ratio"))
    .drop("overall_avg_csat_to_requester_id_ratio")
)

with_activity_2 = (
    with_activity
    .crossJoin(with_activity.agg(F.avg("csat_priority_is_not_null").alias("overall_avg_csat_priority_is_not_null")))
    .filter(F.col("csat_priority_is_not_null") > F.col("overall_avg_csat_priority_is_not_null"))
    .drop("overall_avg_csat_priority_is_not_null")
)

result = (
    with_activity_2
    .filter(F.col("days_since_opened_at_month") < 525)
    .select(
        "closed_at_day",
        "opened_at_month",
        "max_csat",
        "median_csat",
        "csat_priority_is_not_null",
        "csat_to_requester_id_ratio",
        "days_since_opened_at_month",
        F.when(F.col("total_csat") >= 18, "A").when(F.col("total_csat") >= 10, "B").otherwise("C").alias("total_csat_tier"),
    )
    .orderBy("closed_at_day", F.col("median_csat").desc())
)
