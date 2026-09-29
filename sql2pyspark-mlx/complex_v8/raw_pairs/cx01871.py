from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(6, 11)) & (F.col("adjuster_id") <= 6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(1, 3)), "left_anti")
)

tagged = filtered_2.select("*", F.add_months(F.col("claim_date"), 3).alias("claim_date_plus_3m"))

with_activity = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()) & (F.col("c.claim_date") >= F.lit("2024-06-07").cast("date")), "left_anti")
)

result = (
    with_activity
    .select("id", "claimed_amount", "approved_amount_adj")
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount_adj"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
