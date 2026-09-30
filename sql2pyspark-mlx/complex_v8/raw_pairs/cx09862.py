from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_reported_at_running = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_group = Window.partitionBy("status_group")
w_policy_id_by_claim_type_len = Window.partitionBy("policy_id").orderBy(F.col("claim_type_len").desc(), "id")
w_adjuster_id_status_group = Window.partitionBy("adjuster_id", "status_group")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc(), "id")

valid = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(2, 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
)

sequenced = (
    valid
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "claim_type_len",
        "status_group",
        F.lag("claim_date").over(w_all_by_approved_amount).alias("prev_claim_date"),
        F.min("approved_amount").over(w_all_by_reported_at_running).alias("min_approved_amount_so_far"),
        F.round(F.col("claim_type_len") - F.avg("claim_type_len").over(w_status_group), 2).alias("claim_type_len_vs_status_group_avg"),
        F.ntile(3).over(w_policy_id_by_claim_type_len).alias("claim_type_len_tercile"),
        F.lead("approved_amount", 1, 0).over(w_policy_id_by_claim_type_len).alias("next_approved_amount"),
        F.max("claimed_amount").over(w_status_group).alias("status_group_max_claimed_amount"),
        F.min("claimed_amount").over(w_adjuster_id_status_group).alias("adjuster_id_status_group_min_claimed_amount"),
        F.lead("reported_at", 3).over(w_all_by_reported_at).alias("next3_reported_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "status_group",
        "prev_claim_date",
        "claim_type_len_vs_status_group_avg",
        "claim_type_len_tercile",
        "status_group_max_claimed_amount",
        "adjuster_id_status_group_min_claimed_amount",
        "next3_reported_at",
    )
    .orderBy(F.col("id").desc(), F.col("status_group_max_claimed_amount").desc())
    .limit(5)
)
