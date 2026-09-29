from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_claim_type_group = Window.partitionBy("claim_type_group")
w_claim_type_group_policy_id = Window.partitionBy("claim_type_group", "policy_id")
w_claim_type_adjuster_id_by_ap = Window.partitionBy("claim_type", "adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_approved_amount_ties = Window.orderBy("approved_amount")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_approved_amount_2 = Window.orderBy("approved_amount", "id")

valid_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (F.col("adjuster_id") <= 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.when(F.col("claim_type").isin("collision", "flood"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

with_windows = (
    valid_claims
    .select(
        "*",
        F.lag("approved_amount", 1, 0).over(w_all_by_approved_amount).alias("prev_approved_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_group), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_group"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_group_policy_id), 2).alias("approved_amount_vs_claim_type_group_policy_id_av"),
        F.last("approved_amount", ignorenulls=True).over(w_claim_type_adjuster_id_by_ap).alias("last_known_approved_amount"),
        F.max("approved_amount").over(w_claim_type_group_policy_id).alias("claim_type_group_policy_id_max_approved_amount"),
        F.count(F.lit(1)).over(w_claim_type_group).alias("rows_in_claim_type_group"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type_group)).alias("distinct_adjuster_id_in_claim_type_group"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_2), 3).alias("approved_amount_cume_dist"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claim_type_group",
        "claimed_amount_per_approved_amount",
        "prev_approved_amount",
        "approved_amount_vs_claim_type_group_policy_id_av",
        "last_known_approved_amount",
        "rows_in_claim_type_group",
        "approved_amount_percent_rank",
        "distinct_policy_id_in_adjuster_id",
        "distinct_adjuster_id_in_claim_type_group",
        "approved_amount_cume_dist",
        "rows_in_adjuster_id",
    )
    .orderBy(F.col("last_known_approved_amount").desc(), "rows_in_adjuster_id")
)
