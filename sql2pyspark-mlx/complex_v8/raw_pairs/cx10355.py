from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(2, 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
        F.when(F.col("claimed_amount") >= 40638.4, "A").when(F.col("claimed_amount") >= 19609.8, "B").otherwise("C").alias("claimed_amount_tier"),
        F.abs(F.col("claimed_amount") - 20505.7).alias("claimed_amount_distance"),
    )
)

enriched = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2024-06-19").cast("date")), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("p.holder_id") >= 18)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.claim_date_month_end",
        "s.claimed_amount_distance",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.status").alias("policy_status"),
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    enriched
    .select("id", "claimed_amount_distance", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["claimed_amount_distance", "claimed_amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
