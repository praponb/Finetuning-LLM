from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_ties = Window.orderBy(F.col("reported_at").desc())
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")

valid_claims = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-06-26").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

sequenced = (
    valid_claims
    .select(
        "*",
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.size(F.collect_set("policy_id").over(w_claim_type_adjuster_id)).alias("distinct_policy_id_in_claim_type_adjuster_id"),
    )
)

line_level = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(2, 3)), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.reported_at",
        "s.distinct_policy_id_in_claim_type_adjuster_id",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

claim_type_stats = (
    line_level
    .groupBy("claim_type")
    .agg(F.avg("distinct_policy_id_in_claim_type_adjuster_id").alias("avg_distinct_policy_id_in_claim_type_adjuster_id"))
)

vs_group = (
    line_level.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.distinct_policy_id_in_claim_type_adjuster_id") > F.col("g.avg_distinct_policy_id_in_claim_type_adjuster_id"))
    .select(
        "s.*",
        "g.avg_distinct_policy_id_in_claim_type_adjuster_id",
        F.round(F.col("s.distinct_policy_id_in_claim_type_adjuster_id") - F.col("g.avg_distinct_policy_id_in_claim_type_adjuster_id"), 2).alias("distinct_policy_id_in_claim_type_adjuster_id_min"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.distinct_policy_id_in_claim_type_adjuster_id",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.avg_distinct_policy_id_in_claim_type_adjuster_id",
        "s.distinct_policy_id_in_claim_type_adjuster_id_min",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

with_details_policy = (
    joined.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

enriched_claim = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.distinct_policy_id_in_claim_type_adjuster_id",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.avg_distinct_policy_id_in_claim_type_adjuster_id",
        "s.distinct_policy_id_in_claim_type_adjuster_id_min",
        "s.claim_status",
        "s.policy_end_date",
        F.col("c.adjuster_id").alias("claim_adjuster_id_2"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    enriched_claim
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claim_id",
        "claim_adjuster_id",
        "distinct_policy_id_in_claim_type_adjuster_id_min",
        "claim_status",
        "policy_end_date",
        "claim_adjuster_id_2",
        "claim_policy_id",
        F.concat_ws("-", F.col("claim_type"), F.col("claim_status")).alias("claim_type_claim_status_label"),
        F.date_format(F.col("policy_end_date"), "yyyy-MM-dd").alias("policy_end_date_label"),
    )
)
