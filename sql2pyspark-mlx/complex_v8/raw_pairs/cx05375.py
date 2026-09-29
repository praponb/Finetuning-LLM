from pyspark.sql import functions as F

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

joined_policy = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date") < F.lit("2026-11-25").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type",
        "s.status",
        "s.product",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

stats = (
    joined_policy
    .groupBy("claim_type", "policy_id")
    .agg(
        F.round(F.sum("policy_premium"), 2).alias("total_policy_premium"),
        F.count("*").alias("row_count"),
        F.round(100 * F.count(F.when(F.col("claim_type").isin("delay", "flood", "theft"), 1)) / F.count("*"), 1).alias("pct_claim_type_in_delay_flood_th"),
    )
)

result = (
    stats
    .select(
        "claim_type",
        "policy_id",
        "pct_claim_type_in_delay_flood_th",
        "total_policy_premium",
    )
    .unpivot(
        ids=["claim_type", "policy_id"],
        values=["pct_claim_type_in_delay_flood_th", "total_policy_premium"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
