from pyspark.sql import functions as F

base = (
    df_policies
    .filter((F.col("end_date").between(F.lit("2025-11-20").cast("date"), F.lit("2026-08-31").cast("date"))) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

exploded_detail = (
    base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("IN_REVIEW", "PAID", "OPEN")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

with_children_claims = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 31715.7), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.claim_id",
        "s.claim_approved_amount",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        "c.claim_type",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

with_counts = (
    with_children_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.claim_adjuster_id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

result = (
    with_counts
    .groupingSets(
        [["holder_id", "claim_id", "product"], ["holder_id"], []],
        "holder_id", "claim_id", "product",
    )
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.sum("claim_claimed_amount").alias("total_claim_claimed_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.round(F.sum("claim_approved_amount"), 2).alias("total_claim_approved_amount"),
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("claim_id").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
