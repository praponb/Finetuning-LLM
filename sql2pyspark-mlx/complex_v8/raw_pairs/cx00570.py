from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
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

with_children_claims = (
    scoped_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.adjuster_id") >= 3), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

line_level = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.claim_id",
        "s.claim_date",
        "s.claim_adjuster_id",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.status").alias("claim_status_2"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

rollup_base = (
    line_level
    .groupBy("claim_id_2", "holder_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("claim_reported_at_2").alias("first_claim_reported_at_2"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("claim_approved_amount") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("claim_approved_amount_to_premium_ratio"),
    )
)

with_parent_claim = (
    rollup_base.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id_2"), "inner")
    .select(
        "s.claim_id_2",
        "s.holder_id",
        "s.cnt",
        "s.first_claim_reported_at_2",
        "s.status_list",
        "s.distinct_ids",
        "s.claim_approved_amount_to_premium_ratio",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_date",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    with_parent_claim
    .filter(F.col("distinct_ids") < 9)
    .select(
        "claim_id_2",
        "holder_id",
        "first_claim_reported_at_2",
        "status_list",
        "distinct_ids",
        "claim_approved_amount_to_premium_ratio",
        "claim_approved_amount",
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
    .orderBy(F.col("holder_id").desc(), "claim_id_2")
    .limit(10)
)
