from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_level_claim_type = Window.partitionBy("adjuster_level", "claim_type")
w_policy_product_policy_id = Window.partitionBy("policy_product", "policy_id")
w_policy_product_claim_date_mo = Window.partitionBy("policy_product", F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").desc(), "id")
w_all_by_adjuster_level_prior = Window.orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(-3, -1)
w_all_by_claimed_amount_bucket = Window.orderBy("claimed_amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_bucket_2 = Window.orderBy("claimed_amount_bucket", "id")
w_status_by_adjuster_level_tie = Window.partitionBy("status").orderBy(F.col("adjuster_level").desc())

cleaned_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(6, 11))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

with_details = (
    cleaned_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_bucket",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

sequenced = (
    with_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_level_claim_type).alias("rows_in_adjuster_level_claim_type"),
        F.sum("claimed_amount").over(w_policy_product_policy_id).alias("policy_product_policy_id_sum_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_level_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_level_claim_ty"),
        F.lag("claimed_amount", 3, 0).over(w_policy_product_claim_date_mo).alias("prev3_claimed_amount"),
        F.max("claimed_amount_bucket").over(w_all_by_adjuster_level_prior).alias("claimed_amount_bucket_max_3_prior"),
        F.nth_value("policy_end_date", 2).over(w_all_by_claimed_amount_bucket).alias("second_policy_end_date"),
        F.row_number().over(w_all_by_claimed_amount_bucket_2).alias("rn"),
        F.rank().over(w_status_by_adjuster_level_tie).alias("adjuster_level_rank"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.quarter(F.col("second_policy_end_date")).alias("second_policy_end_date_quarter"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("claimed_amount_bucket").cast("string")).alias("claim_type_ref"),
        F.concat_ws("|", F.col("claim_type"), F.col("policy_product")).alias("claim_type_policy_product_label"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "claimed_amount_bucket",
        "policy_end_date",
        "policy_holder_id",
        "policy_product",
        "adjuster_level",
        "adjuster_team",
        "rows_in_adjuster_level_claim_type",
        "policy_product_policy_id_sum_claimed_amount",
        "approved_amount_share_of_adjuster_level_claim_ty",
        "prev3_claimed_amount",
        "second_policy_end_date",
        "rn",
        "adjuster_level_rank",
        "second_policy_end_date_quarter",
        "claim_type_ref",
        "claim_type_policy_product_label",
        F.add_months(F.col("policy_end_date"), 12).alias("policy_end_date_plus_12m"),
    )
    .orderBy(
        F.col("approved_amount").desc(),
        F.col("approved_amount_share_of_adjuster_level_claim_ty").desc(),
    )
)
