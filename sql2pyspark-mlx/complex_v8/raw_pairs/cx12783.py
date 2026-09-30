from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id_by_rep = Window.partitionBy("adjuster_id", "policy_id").orderBy("reported_at")
w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_status_reported_at_year_by_r = Window.partitionBy("status", F.year("reported_at")).orderBy(F.col("reported_at").asc_nulls_last())

scoped_claims = (
    df_claims
    .filter(F.col("claim_type") != "collision")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

sequenced = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        F.rank().over(w_adjuster_id_policy_id_by_rep).alias("reported_at_rank"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_status)).alias("distinct_policy_id_in_adjuster_id_status"),
        F.rank().over(w_status_reported_at_year_by_r).alias("reported_at_rank_2"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_status), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_status"),
    )
)

with_details_policy = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.holder_id") <= 15)
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.reported_at",
        "s.reported_at_rank",
        "s.distinct_policy_id_in_adjuster_id_status",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    with_details_policy
    .rollup("distinct_policy_id_in_adjuster_id_status", "reported_at_rank")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.round(F.sum("reported_at_rank") / F.nullif(F.sum("distinct_policy_id_in_adjuster_id_status"), F.lit(0)), 3).alias("reported_at_rank_to_distinct_policy_id_in_adjust"),
    )
    .orderBy(
        F.col("distinct_policy_id_in_adjuster_id_status").asc_nulls_last(),
        F.col("reported_at_rank").asc_nulls_last(),
    )
)
