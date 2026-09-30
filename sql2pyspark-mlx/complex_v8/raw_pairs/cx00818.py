from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.month("claim_date"))
w_reported_at_year_by_approved = Window.partitionBy("reported_at_year").orderBy(F.col("approved_amount_plus_claimed_amount").desc_nulls_first(), "id")

prepared = (
    df_claims
    .filter(F.col("policy_id") >= 15)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.year(F.col("reported_at")).alias("reported_at_year"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount_plus_claimed_amount",
        "claim_type_len",
        "reported_at_year",
        F.round(F.avg("approved_amount").over(w_policy_id_claim_date_month), 2).alias("policy_id_claim_date_month_avg_approved_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_reported_at_year_by_approved)).alias("days_since_prev_claim_date"),
        F.rank().over(w_reported_at_year_by_approved).alias("approved_amount_plus_claimed_amount_rank"),
        F.dense_rank().over(w_reported_at_year_by_approved).alias("approved_amount_plus_claimed_amount_dense_rank"),
    )
)

with_parent_policy = (
    metrics.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type",
        "s.approved_amount_plus_claimed_amount",
        "s.claim_type_len",
        "s.reported_at_year",
        "s.policy_id_claim_date_month_avg_approved_amount",
        "s.approved_amount_plus_claimed_amount_rank",
        "s.approved_amount_plus_claimed_amount_dense_rank",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

qualified = (
    with_parent_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["policy_holder_id", "claim_type_len", "policy_id"], ["policy_holder_id"], ["claim_type_len"], []],
        "policy_holder_id", "claim_type_len", "policy_id",
    )
    .agg(
        F.sum(F.when(F.col("claim_type") == "delay", F.col("claim_type_len"))).alias("claim_type_len_claim_type_delay"),
        F.round(100 * F.count(F.when(F.col("claim_type") != "fire", 1)) / F.count("*"), 1).alias("pct_claim_type_fire"),
    )
    .orderBy(
        F.col("policy_holder_id").asc_nulls_last(),
        F.col("claim_type_len").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
