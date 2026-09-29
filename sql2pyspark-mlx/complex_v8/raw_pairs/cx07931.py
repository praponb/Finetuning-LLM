from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount_minus_claimed_amount", "id")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount_minus_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_approved_amount_3 = Window.partitionBy("policy_id").orderBy("approved_amount_minus_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_type = Window.partitionBy("status", "claim_type")

cleaned = (
    df_claims
    .filter((~F.col("status").isin("REJECTED", "OPEN")) | (F.col("approved_amount").between(0.0, 39988.0)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_by_approved_amount)).alias("claimed_amount_change"),
        F.nth_value("claimed_amount", 2).over(w_policy_id_by_approved_amount_2).alias("second_claimed_amount"),
        F.sum("claimed_amount").over(w_policy_id_by_approved_amount_3).alias("running_claimed_amount"),
        F.count(F.lit(1)).over(w_status_claim_type).alias("rows_in_status_claim_type"),
        F.ntile(4).over(w_policy_id_by_approved_amount).alias("approved_amount_minus_claimed_amount_quartile"),
    )
)

enriched_policy = (
    with_windows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    enriched_policy
    .groupingSets(
        [["approved_amount_minus_claimed_amount_quartile", "adjuster_id", "policy_holder_id"], ["approved_amount_minus_claimed_amount_quartile"], []],
        "approved_amount_minus_claimed_amount_quartile", "adjuster_id", "policy_holder_id",
    )
    .agg(
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.round(F.sum("rows_in_status_claim_type") / F.nullif(F.sum("approved_amount_minus_claimed_amount_quartile"), F.lit(0)), 3).alias("rows_in_status_claim_type_to_approved_amount_min"),
    )
    .orderBy(
        F.col("approved_amount_minus_claimed_amount_quartile").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_holder_id").asc_nulls_last(),
    )
)
