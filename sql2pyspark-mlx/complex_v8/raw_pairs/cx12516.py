from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

with_details_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") >= 2)
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

screened = (
    with_details_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") > 0.0), "left_semi")
)

result = (
    screened
    .select(
        "id",
        F.col("claimed_amount_bucket").alias("m_claimed_amount_bucket"),
        F.col("adjuster_level").alias("m_adjuster_level"),
    )
    .unpivot(
        ids=["id"],
        values=["m_claimed_amount_bucket", "m_adjuster_level"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "metric")
)
