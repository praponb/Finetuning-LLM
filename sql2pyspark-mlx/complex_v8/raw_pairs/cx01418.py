from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_holder_id_holder_segm = Window.partitionBy("policy_holder_id", "holder_segment").orderBy(F.col("claim_date").desc(), "id")
w_all_by_prior_rows_90d_full = Window.orderBy("prior_rows_90d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 90)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_90d"),
        F.sum("b.claimed_amount").alias("prior_claimed_amount_90d"),
    )
)

with_details = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.segment").alias("holder_segment"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_claimed_amount_90d",
        "policy_premium",
        "policy_holder_id",
        "holder_segment",
        F.lag("policy_premium").over(w_policy_holder_id_holder_segm).alias("prev_policy_premium"),
        F.first("prior_rows_90d").over(w_all_by_prior_rows_90d_full).alias("first_prior_rows_90d"),
    )
)

per_group = (
    with_windows
    .filter(F.col("policy_holder_id").isNotNull())
    .groupBy("policy_id", "policy_holder_id")
    .agg(
        F.max("first_prior_rows_90d").alias("max_first_prior_rows_90d"),
        F.max("claim_date").alias("last_claim_date"),
        F.max("prior_claimed_amount_90d").alias("max_prior_claimed_amount_90d"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    per_group
    .groupingSets(
        [["cnt", "max_first_prior_rows_90d", "policy_id"], ["cnt"]],
        "cnt", "max_first_prior_rows_90d", "policy_id",
    )
    .agg(
        F.round(F.sum("max_prior_claimed_amount_90d") / F.nullif(F.sum("max_first_prior_rows_90d"), F.lit(0)), 3).alias("max_prior_claimed_amount_90d_to_max_first_prior_"),
    )
)
