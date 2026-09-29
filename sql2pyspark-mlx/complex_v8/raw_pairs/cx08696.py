from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-11-20").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    base_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

with_details_holder = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.product_prefix",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

summary = (
    with_details_holder
    .groupBy(
        F.date_format("end_date", "yyyy-MM").alias("end_date_ym"),
        "holder_id",
        F.year("start_date").alias("start_date_year"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("holder_risk_score"), 2).alias("stddev_holder_risk_score"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.count("*").alias("cnt"),
        F.avg("premium").alias("avg_premium_check"),
    )
    .filter(F.col("avg_premium_check") > 1132.1)
    .drop("avg_premium_check")
)

result = (
    summary
    .select(
        "end_date_ym",
        "holder_id",
        "end_date_span_days",
        "distinct_ids",
        "start_date_year",
        "distinct_holder_ids",
    )
    .unpivot(
        ids=["end_date_ym", "holder_id"],
        values=["end_date_span_days", "distinct_ids", "start_date_year", "distinct_holder_ids"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
