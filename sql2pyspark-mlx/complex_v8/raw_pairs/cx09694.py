from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.sum("claimed_amount").alias("total_claim_claimed_amount"))
)

with_counts = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.total_claim_claimed_amount")
)

adjuster_id_stats = (
    with_counts
    .groupBy("adjuster_id")
    .agg(F.max("approved_amount").alias("max_approved_amount"))
)

above_average = (
    with_counts.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") == F.col("g.max_approved_amount"))
    .select(
        "s.*",
        "g.max_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.max_approved_amount"), 2).alias("approved_amount_minus_max"),
    )
)

filtered = (
    above_average.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "LAPSED") & (F.col("p.product") == "LIFE"), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "approved_amount_minus_max",
        "claimed_amount",
        "greatest_claimed_amount_approved_amount",
        "max_approved_amount",
    )
    .unpivot(
        ids=["id"],
        values=["approved_amount_minus_max", "claimed_amount", "greatest_claimed_amount_approved_amount", "max_approved_amount"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
