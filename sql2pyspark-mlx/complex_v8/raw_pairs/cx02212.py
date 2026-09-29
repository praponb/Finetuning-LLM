from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("status").isin("PAID", "APPROVED")) | (F.col("claim_type") == "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

filtered_2 = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp")) & (F.col("c.claimed_amount") < 31752.9), "left_semi")
    .filter(F.col("claim_date_label").isNotNull())
)

result = (
    filtered_2
    .groupingSets(
        [["policy_id", "adjuster_id"], ["policy_id"], ["adjuster_id"]],
        "policy_id", "adjuster_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)
