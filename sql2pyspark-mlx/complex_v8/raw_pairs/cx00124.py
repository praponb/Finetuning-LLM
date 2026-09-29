from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

sequenced = (
    in_window
    .select(
        "*",
        F.max("premium").over(w_policy_id).alias("policy_id_max_premium"),
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_policy_id), F.lit(0)), 4).alias("premium_share_of_policy_id"),
    )
)

result = (
    sequenced
    .filter(F.col("distinct_claim_type_in_policy_id") >= 3)
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "adjuster_id",
        "product",
        "premium",
        "policy_id_max_premium",
        "policy_id_sum_claimed_amount",
        "distinct_claim_type_in_policy_id",
        "rows_in_policy_id",
        "premium_share_of_policy_id",
    )
    .orderBy("claim_type", "adjuster_id", "id")
    .limit(20)
)
