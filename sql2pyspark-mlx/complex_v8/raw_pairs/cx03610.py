from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_adjuster_id_policy_product = Window.partitionBy("adjuster_id", "policy_product")
w_policy_product_by_reported_a = Window.partitionBy("policy_product").orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_claim_type_by_least_approved = Window.partitionBy("claim_type").orderBy("least_approved_amount_claimed_amount", "id")
w_policy_product_by_reported_a_2 = Window.partitionBy("policy_product").orderBy(F.col("reported_at").desc_nulls_first())
w_policy_product = Window.partitionBy("policy_product")
w_claim_type_by_least_approved_2 = Window.partitionBy("claim_type").orderBy("least_approved_amount_claimed_amount")
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_claims = (
    df_claims
    .filter(~F.col("claim_type").isin("theft", "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

enriched_policy = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_windows = (
    enriched_policy
    .select(
        "*",
        F.rank().over(w_all_by_reported_at_ties).alias("reported_at_rank"),
        F.lead("policy_start_date").over(w_all_by_reported_at).alias("next_policy_start_date"),
        F.dense_rank().over(w_all_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_adjuster_id_policy_product).alias("rows_in_adjuster_id_policy_product"),
        F.lag("approved_amount", 2).over(w_policy_product_by_reported_a).alias("prev2_approved_amount"),
        F.round(F.cume_dist().over(w_claim_type_by_least_approved), 3).alias("least_approved_amount_claimed_amount_cume_dist"),
        F.dense_rank().over(w_policy_product_by_reported_a_2).alias("reported_at_dense_rank_2"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_product), F.lit(0)), 4).alias("approved_amount_share_of_policy_product"),
        F.dense_rank().over(w_claim_type_by_least_approved_2).alias("least_approved_amount_claimed_amount_dense_rank"),
        F.nth_value("approved_amount", 2).over(w_all_by_reported_at_full).alias("second_approved_amount"),
        F.min("approved_amount").over(w_all_by_reported_at_running).alias("min_approved_amount_so_far"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_product), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_prod"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_reported_at))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "reported_at",
        "least_approved_amount_claimed_amount",
        "policy_product",
        "policy_start_date",
        "reported_at_rank",
        "next_policy_start_date",
        "reported_at_dense_rank",
        "rows_in_adjuster_id_policy_product",
        "prev2_approved_amount",
        "least_approved_amount_claimed_amount_cume_dist",
        "reported_at_dense_rank_2",
        "approved_amount_share_of_policy_product",
        "least_approved_amount_claimed_amount_dense_rank",
        "second_approved_amount",
        "approved_amount_share_of_adjuster_id_policy_prod",
        F.date_format(F.col("policy_start_date"), "yyyy-MM-dd").alias("policy_start_date_label"),
    )
    .orderBy(
        F.col("least_approved_amount_claimed_amount_dense_rank").asc(),
        "next_policy_start_date",
        "id",
    )
    .limit(10)
)
