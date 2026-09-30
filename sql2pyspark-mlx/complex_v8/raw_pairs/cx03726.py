from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

derived = (
    scoped
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "claim_type_slug",
        "claimed_amount_adj",
        F.when(F.col("claimed_amount") >= 31715.7, "large").when(F.col("claimed_amount") >= 17759.3, "medium").otherwise("small").alias("claimed_amount_tier"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.round(F.col("claimed_amount_adj") * 1.07, 2).alias("claimed_amount_adj_adj"),
    )
)

result = (
    derived
    .select(
        "id",
        F.col("claimed_amount").alias("claimed_amount_v"),
        F.col("claimed_amount_filled").alias("m_claimed_amount_filled"),
        F.col("claimed_amount_adj_adj").alias("m_claimed_amount_adj_adj"),
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount_v", "m_claimed_amount_filled", "m_claimed_amount_adj_adj"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
