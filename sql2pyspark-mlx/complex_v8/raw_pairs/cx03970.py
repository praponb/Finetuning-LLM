from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_filled").desc(), "id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_all_by_claimed_amount_filled = Window.orderBy(F.col("claimed_amount_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_filled_2 = Window.orderBy(F.col("claimed_amount_filled").desc())

scoped_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

sequenced = (
    scoped_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.last("claimed_amount").over(w_adjuster_id_by_claimed_amoun).alias("last_claimed_amount"),
        F.lead("claimed_amount").over(w_adjuster_id_by_claimed_amoun_2).alias("next_claimed_amount"),
        F.round(F.avg("claimed_amount_filled").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount_filled"),
    )
)

filtered = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type").isin("delay", "theft", "fire")) & (F.month(F.col("c.reported_at")).isin(1, 3)), "left_semi")
)

sequenced_2 = (
    filtered
    .select(
        "*",
        F.sum("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_sum_claimed_amount"),
        F.first("claimed_amount_filled").over(w_all_by_claimed_amount_filled).alias("first_claimed_amount_filled"),
        F.rank().over(w_all_by_claimed_amount_filled_2).alias("claimed_amount_filled_rank"),
    )
)

result = (
    sequenced_2
    .rollup("adjuster_id", "claimed_amount_filled_rank")
    .agg(F.countDistinct("policy_id").alias("distinct_policy_ids"))
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claimed_amount_filled_rank").asc_nulls_last(),
    )
)
