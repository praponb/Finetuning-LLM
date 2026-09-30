from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_status = Window.partitionBy("status")
w_status_by_claimed_amount_per = Window.partitionBy("status").orderBy(F.col("claimed_amount_per_approved_amount").desc(), "id").rowsBetween(-3, Window.currentRow)
w_claim_type_by_policy_start_d = Window.partitionBy("claim_type").orderBy("policy_start_date", "id")
w_status_by_claimed_amount_per_2 = Window.partitionBy("status").orderBy(F.col("claimed_amount_per_approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_claim_date_year_claim_type_b = Window.partitionBy(F.year("claim_date"), "claim_type").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(-3, -1)
w_policy_product_policy_end_da = Window.partitionBy("policy_product", F.date_trunc("month", "policy_end_date")).orderBy(F.col("approved_amount_avg_3_prior").desc(), "id").rowsBetween(-3, -1)
w_policy_product_policy_end_da_2 = Window.partitionBy("policy_product", F.date_trunc("month", "policy_end_date")).orderBy(F.col("approved_amount_avg_3_prior").desc(), "id")
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")

base_claims = (
    df_claims
    .filter(F.col("approved_amount") > 7204.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

combined_details_policy = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isin("LAPSED", "EXPIRED")), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .filter(F.col("p.end_date").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_per_approved_amount",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

with_windows = (
    combined_details_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount_per_approved_amount",
        "policy_end_date",
        "policy_product",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_adjuster_id"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.max("approved_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_max_approved_amount"),
        F.round(F.avg("approved_amount").over(w_status_by_claimed_amount_per), 2).alias("approved_amount_avg_4"),
        F.max("approved_amount").over(w_status).alias("status_max_approved_amount"),
        F.row_number().over(w_claim_type_by_policy_start_d).alias("rn"),
        F.max("approved_amount").over(w_status_by_claimed_amount_per_2).alias("approved_amount_max_3"),
        F.round(F.avg("approved_amount").over(w_claim_date_year_claim_type_b), 2).alias("approved_amount_avg_3_prior"),
    )
)

top_rows = (
    with_windows
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

ranked = (
    top_rows
    .select(
        "*",
        F.max("rows_in_status").over(w_policy_product_policy_end_da).alias("rows_in_status_max_3_prior"),
        F.dense_rank().over(w_policy_product_policy_end_da_2).alias("approved_amount_avg_3_prior_dense_rank"),
        F.count(F.lit(1)).over(w_claim_type_policy_id).alias("rows_in_claim_type_policy_id"),
        F.round(F.avg("status_max_approved_amount").over(w_claim_type_policy_id), 2).alias("claim_type_policy_id_avg_status_max_approved_amo"),
    )
)

result = (
    ranked
    .select(
        "status_max_approved_amount",
        "id",
        "approved_amount_share_of_claim_type_adjuster_id",
    )
    .distinct()
    .orderBy(
        F.col("approved_amount_share_of_claim_type_adjuster_id").desc_nulls_first(),
        F.col("id").desc_nulls_first(),
    )
)
