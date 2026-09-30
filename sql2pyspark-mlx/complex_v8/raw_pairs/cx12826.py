from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter((F.col("product") != "AUTO") & (F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2027-01-03").cast("date"))))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("end_date")).alias("days_since_end_date"),
    )
)

status_stats = (
    cleaned
    .groupBy("status")
    .agg(F.avg("premium").alias("avg_premium"))
)

compared = (
    cleaned.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

with_parent = (
    compared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.premium_filled",
        "s.status_slug",
        "s.days_since_end_date",
        "s.avg_premium",
        "s.premium_minus_avg",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.col("p.segment") == "SME")
    .select("s.*", F.col("p.segment").alias("holder_segment"))
)

result = (
    with_details
    .select(
        "id",
        "premium_filled",
        "premium",
        "premium_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["premium_filled", "premium", "premium_minus_avg"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric")
)
