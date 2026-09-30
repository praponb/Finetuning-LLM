from pyspark.sql import functions as F

recent = (
    df_policies
    .filter(~F.col("product").isin("AUTO", "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("end_date"), "MMM yyyy").alias("end_date_label"),
        F.when(F.col("status").isin("EXPIRED", "ACTIVE"), "group_a").when(F.col("status") == "CANCELLED", "group_b").otherwise("other").alias("status_group"),
    )
)

filtered = (
    recent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") >= F.lit("2025-11-20").cast("date")) & (F.col("p.status") == "ACTIVE"), "left_anti")
    .filter(F.year(F.col("end_date")) == 2025)
)

with_details_holder = (
    filtered.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.end_date",
        "s.end_date_label",
        "s.status_group",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

exploded_detail = (
    with_details_holder.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status") != "APPROVED"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.status_group",
        "s.holder_joined_on",
        "s.holder_risk_score",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

combined_details_claim = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("c.policy_id"), "left")
    .select(
        "s.*",
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    combined_details_claim
    .filter(F.col("claim_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "status_group",
        "holder_joined_on",
        "holder_risk_score",
        "claim_status",
        "claim_adjuster_id",
        "claim_adjuster_id_2",
        "claim_approved_amount",
        "policy_product",
        "policy_start_date",
    )
    .orderBy(F.col("policy_start_date").asc(), F.col("claim_adjuster_id").desc())
)
