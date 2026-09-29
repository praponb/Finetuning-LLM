from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id_by_ap = Window.partitionBy("claim_type", "adjuster_id").orderBy("approved_amount_minus_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_claim_type_by_claim_date_pri = Window.partitionBy("claim_type").orderBy("claim_date", "id").rowsBetween(-3, -1)
w_claim_type_adjuster_id_by_ap_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy("approved_amount_minus_claimed_amount", "id").rowsBetween(-1, Window.currentRow)
w_claim_type_adjuster_id_by_ap_3 = Window.partitionBy("claim_type", "adjuster_id").orderBy("approved_amount_minus_claimed_amount", "id")
w_status = Window.partitionBy("status")
w_claim_type_by_claim_date = Window.partitionBy("claim_type").orderBy("claim_date", "id")
w_claim_type_claim_date_month = Window.partitionBy("claim_type", F.month("claim_date")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_claims
    .filter((F.col("policy_id") >= 19) & (F.col("claim_date") < F.lit("2025-09-19").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

with_parent = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.product").alias("policy_product"))
)

ranked = (
    with_parent
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "approved_amount_minus_claimed_amount",
        F.max("claimed_amount").over(w_claim_type_adjuster_id_by_ap).alias("max_claimed_amount_so_far"),
        F.round(F.avg("claimed_amount").over(w_policy_id_claim_type), 2).alias("policy_id_claim_type_avg_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_claim_type_by_claim_date_pri), 2).alias("claimed_amount_avg_3_prior"),
        F.max("claimed_amount").over(w_claim_type_adjuster_id_by_ap_2).alias("claimed_amount_max_2"),
        F.lead("claimed_amount").over(w_claim_type_adjuster_id_by_ap_3).alias("next_claimed_amount"),
        F.row_number().over(w_claim_type_adjuster_id_by_ap_3).alias("approved_amount_minus_claimed_amount_seq"),
        F.sum("claimed_amount").over(w_status).alias("status_sum_claimed_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.last("claimed_amount", ignorenulls=True).over(w_claim_type_claim_date_month).alias("last_known_claimed_amount"),
    )
)

derived = (
    ranked
    .filter(F.col("policy_id_claim_type_avg_claimed_amount") < 45010.0)
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "max_claimed_amount_so_far",
        "policy_id_claim_type_avg_claimed_amount",
        "claimed_amount_avg_3_prior",
        "next_claimed_amount",
        "approved_amount_minus_claimed_amount_seq",
        "status_sum_claimed_amount",
        "claim_date_percent_rank",
        "last_known_claimed_amount",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

result = (
    derived
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "max_claimed_amount_so_far",
        "policy_id_claim_type_avg_claimed_amount",
        "claimed_amount_avg_3_prior",
        "next_claimed_amount",
        "approved_amount_minus_claimed_amount_seq",
        "status_sum_claimed_amount",
    )
    .orderBy(
        "policy_id_claim_type_avg_claimed_amount",
        F.col("status_sum_claimed_amount").asc_nulls_last(),
    )
)
