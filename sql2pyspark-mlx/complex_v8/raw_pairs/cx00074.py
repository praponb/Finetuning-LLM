from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(1, 2)) & (F.hour(F.col("opened_at")).between(10, 14)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

totals = (
    filtered
    .filter(F.col("csat") >= 2)
    .groupBy("csat")
    .agg(
        F.max("days_since_opened_at").alias("max_days_since_opened_at"),
        F.count("*").alias("n_rows"),
        F.sum("csat").alias("total_csat"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
    )
)

with_activity = (
    totals
    .crossJoin(totals.agg(F.avg("total_csat").alias("overall_avg_total_csat")))
    .filter(F.col("total_csat") > F.col("overall_avg_total_csat"))
    .drop("overall_avg_total_csat")
)

annotated = (
    with_activity
    .filter(F.col("csat") <= 4)
    .select(
        "csat",
        "n_rows",
        "total_csat",
        "priority_list",
        (F.col("n_rows") - F.col("total_csat")).alias("n_rows_minus_total_csat"),
        (F.floor(F.col("total_csat") / 5) * 5).alias("total_csat_bucket"),
        F.round(F.col("csat") / F.nullif(F.col("max_days_since_opened_at"), F.lit(0)), 3).alias("csat_per_max_days_since_opened_at"),
    )
)

screened = (
    annotated
    .crossJoin(annotated.agg(F.max("total_csat").alias("overall_max_total_csat")))
    .filter(F.col("total_csat") < F.col("overall_max_total_csat"))
    .drop("overall_max_total_csat")
)

result = (
    screened
    .filter(F.col("n_rows_minus_total_csat").between(9, 38))
    .select(
        "csat",
        "n_rows_minus_total_csat",
        F.when(F.col("total_csat") >= 23, "HIGH").when(F.col("total_csat") >= 7, "MEDIUM").otherwise("LOW").alias("total_csat_tier"),
        F.round(F.col("total_csat_bucket") / F.nullif(F.col("total_csat"), F.lit(0)), 3).alias("total_csat_bucket_per_total_csat"),
    )
    .orderBy(
        F.col("n_rows_minus_total_csat").asc(),
        F.col("total_csat_bucket_per_total_csat").desc(),
        "csat",
    )
    .limit(10)
)
