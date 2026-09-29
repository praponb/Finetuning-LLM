from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-10-12").cast("date"), F.lit("2027-01-03").cast("date"))) & (F.col("status").isin("ACTIVE", "LAPSED")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

combined_details_holder = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

profiled = (
    combined_details_holder.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.first_claim_date", "c.total_claim_approved_amount")
)

per_group = (
    profiled
    .groupBy(
        "holder_segment",
        F.date_trunc("month", "first_claim_date").alias("first_claim_date_month"),
    )
    .agg(
        F.count(F.when(~F.col("holder_segment").isin("SME", "VIP"), 1)).alias("n_holder_segment_not_in_sme_vi"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.count("*").alias("n_rows"),
        F.max("holder_risk_score").alias("max_holder_risk_score"),
    )
)

scored = (
    per_group
    .filter(F.col("avg_premium") < 4280.0)
    .select(
        "holder_segment",
        "first_claim_date_month",
        "n_holder_segment_not_in_sme_vi",
        "avg_premium",
        "n_rows",
        "max_holder_risk_score",
        F.year(F.col("first_claim_date_month")).alias("first_claim_date_month_year"),
        F.substring(F.trim(F.col("holder_segment")), 1, 3).alias("holder_segment_prefix"),
    )
)

qualified = (
    scored
    .crossJoin(scored.agg(F.min("n_holder_segment_not_in_sme_vi").alias("overall_min_n_holder_segment_not_in_sme_vi")))
    .filter(F.col("n_holder_segment_not_in_sme_vi") > F.col("overall_min_n_holder_segment_not_in_sme_vi"))
    .drop("overall_min_n_holder_segment_not_in_sme_vi")
)

result = (
    qualified
    .select(
        "holder_segment",
        "n_holder_segment_not_in_sme_vi",
        "avg_premium",
        "n_rows",
        "max_holder_risk_score",
        "first_claim_date_month_year",
        F.round(F.col("avg_premium") * 1.5, 2).alias("avg_premium_adj"),
        F.date_trunc("week", F.col("first_claim_date_month")).alias("first_claim_date_month_week"),
    )
    .orderBy("n_holder_segment_not_in_sme_vi", F.col("n_rows").desc())
)
