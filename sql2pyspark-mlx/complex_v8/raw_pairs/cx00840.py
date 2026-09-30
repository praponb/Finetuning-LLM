from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2025)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
    )
)

with_details_holder = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(F.upper(F.trim(F.col("p.holder_name"))).like("N%"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

detail_rows = (
    with_details_holder.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        "c.claim_type",
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "holder_id",
        "start_date",
        "claim_date",
        "claim_adjuster_id",
    )
    .orderBy(F.col("claim_date").desc())
)
