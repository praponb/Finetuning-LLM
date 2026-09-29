from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_bucket").desc(), "id")
w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_policy_holder_id_b = Window.partitionBy("policy_id", "policy_holder_id").orderBy("policy_end_date")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_policy_holder_id_b_2 = Window.partitionBy("policy_id", "policy_holder_id").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_policy_holder_id_b_3 = Window.partitionBy("policy_id", "policy_holder_id").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("claim_type").isin("flood", "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

with_windows = (
    prepared_claims
    .select(
        "*",
        F.sum("approved_amount").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_sum_approved_amount"),
        F.dense_rank().over(w_adjuster_id_by_approved_amou).alias("approved_amount_bucket_dense_rank"),
        F.max("claimed_amount").over(w_policy_id_by_claimed_amount).alias("max_claimed_amount_so_far"),
    )
)

combined_details = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.approved_amount_bucket",
        "s.max_claimed_amount_so_far",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "policy_id",
        "approved_amount_bucket",
        "policy_holder_id",
        "policy_end_date",
        F.rank().over(w_policy_id_policy_holder_id_b).alias("policy_end_date_rank"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
        F.nth_value("holder_risk_score", 2).over(w_policy_id_policy_holder_id_b_2).alias("second_holder_risk_score"),
        F.round(F.cume_dist().over(w_policy_id_policy_holder_id_b), 3).alias("policy_end_date_cume_dist"),
        F.last("max_claimed_amount_so_far", ignorenulls=True).over(w_policy_id_policy_holder_id_b_3).alias("last_known_max_claimed_amount_so_far"),
    )
)

result = (
    sequenced
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "approved_amount_bucket",
        "policy_end_date",
        "last_known_max_claimed_amount_so_far",
        F.date_format(F.col("policy_end_date"), "yyyy-MM").alias("policy_end_date_label"),
    )
    .orderBy(F.col("policy_id").desc(), F.col("policy_end_date").desc(), "id")
    .limit(10)
)
