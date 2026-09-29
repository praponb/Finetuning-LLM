from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("claimed_amount") > 11235.6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

derived = (
    recent
    .select(
        "*",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
        F.date_trunc("quarter", F.col("reported_at")).alias("reported_at_quarter"),
    )
)

screened = (
    derived.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.year(F.col("c.claim_date")) == 2024) & (F.month(F.col("c.reported_at")).isin(1, 3)), "left_semi")
)

result = (
    screened
    .groupBy("adjuster_id", "status")
    .pivot("claim_type", ["collision", "flood"])
    .agg(F.count("approved_amount"))
    .orderBy("adjuster_id", "status")
)
