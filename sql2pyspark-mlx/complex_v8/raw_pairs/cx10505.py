from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("status") == "EXPIRED") & (F.col("product").isin("HEALTH", "AUTO")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

enriched = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
    )
)

detail_rows = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "holder_segment",
        "claim_date",
        "claim_status",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
    .orderBy(F.col("claim_status").desc_nulls_first(), F.col("holder_id").desc())
)
