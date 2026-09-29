from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("product") == "AUTO") & (F.col("premium").between(1042.5, 3719.7)))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
    )
)

with_parent_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.holder_name").isNotNull()), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status",
        F.col("p.segment").alias("holder_segment"),
    )
)

exploded_detail_claims = (
    with_parent_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") >= 20436.5), "inner")
    .select(
        "s.id",
        "s.holder_id",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    exploded_detail_claims
    .cube("claim_id", "claim_adjuster_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("claim_status") == "APPROVED", 1).otherwise(0)).alias("n_claim_status_approved"),
        F.min("claim_claimed_amount").alias("min_claim_claimed_amount"),
    )
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_adjuster_id").asc_nulls_last(),
    )
)
