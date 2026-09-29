from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"))
)

profiled = (
    matched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.avg_claim_claimed_amount")
)

policy_id_stats = (
    profiled
    .groupBy("policy_id")
    .agg(F.avg("prior_approved_amount_60d").alias("avg_prior_approved_amount_60d"))
)

outliers = (
    profiled.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_approved_amount_60d") > F.col("g.avg_prior_approved_amount_60d"))
    .select(
        "s.*",
        "g.avg_prior_approved_amount_60d",
        F.round(F.col("s.prior_approved_amount_60d") - F.col("g.avg_prior_approved_amount_60d"), 2).alias("prior_approved_amount_60d_minus_avg"),
    )
)

result = (
    outliers
    .select("id", "avg_claim_claimed_amount", "avg_prior_approved_amount_60d")
    .unpivot(
        ids=["id"],
        values=["avg_claim_claimed_amount", "avg_prior_approved_amount_60d"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
