from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claim_date_running = Window.partitionBy("status").orderBy(F.col("claim_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_year = Window.partitionBy(F.year("claim_date"))
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_approved_amount_ties = Window.orderBy("approved_amount")
w_claim_type_group_by_first_kn = Window.partitionBy("claim_type_group").orderBy("first_known_claimed_amount", "id")
w_approved_amount_seq_reported = Window.partitionBy("approved_amount_seq", F.month("reported_at")).orderBy(F.col("approved_amount_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_claims
    .filter(F.month(F.col("reported_at")).isin(1, 3))
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
        F.when(F.col("claim_type").isin("flood", "collision"), "group_a").when(F.col("claim_type") == "theft", "group_b").otherwise("other").alias("claim_type_group"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.first("claimed_amount", ignorenulls=True).over(w_status_by_claim_date_running).alias("first_known_claimed_amount"),
        F.size(F.collect_set("claim_type_group").over(w_claim_date_year)).alias("distinct_claim_type_group_in_claim_date_year"),
        F.row_number().over(w_all_by_approved_amount).alias("approved_amount_seq"),
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.round(F.percent_rank().over(w_claim_type_group_by_first_kn), 3).alias("first_known_claimed_amount_percent_rank"),
        F.lag("approved_amount_cume_dist", 1, 0).over(w_claim_type_group_by_first_kn).alias("prev_approved_amount_cume_dist"),
        F.first("approved_amount_cume_dist", ignorenulls=True).over(w_approved_amount_seq_reported).alias("first_known_approved_amount_cume_dist"),
    )
)

with_details_policy = (
    metrics.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_group",
        "s.first_known_claimed_amount",
        "s.distinct_claim_type_group_in_claim_date_year",
        "s.approved_amount_seq",
        "s.approved_amount_cume_dist",
        "s.first_known_claimed_amount_percent_rank",
        "s.first_known_approved_amount_cume_dist",
        F.col("p.product").alias("policy_product"),
    )
)

without_matches = (
    with_details_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("adjuster_id").isNotNull())
)

result = (
    without_matches
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "distinct_claim_type_group_in_claim_date_year",
        "approved_amount_seq",
        "approved_amount_cume_dist",
        "first_known_claimed_amount_percent_rank",
        "first_known_approved_amount_cume_dist",
        "policy_product",
        F.concat(F.col("policy_product"), F.lit("#"), F.col("id").cast("string")).alias("policy_product_ref"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
    .orderBy(
        F.col("policy_product").asc(),
        F.col("approved_amount_cume_dist").desc(),
    )
)
