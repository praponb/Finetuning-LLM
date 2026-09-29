from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_reported_at_ties = Window.partitionBy("status").orderBy(F.col("reported_at").desc())
w_policy_id = Window.partitionBy("policy_id")
w_status_by_reported_at = Window.partitionBy("status").orderBy(F.col("reported_at").desc(), "id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_adjuster_id_reported_at_date = Window.partitionBy("adjuster_id", F.year("reported_at_date")).orderBy(F.col("reported_at_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_reported_at_full = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_all_by_approved_amount_prior = Window.orderBy("approved_amount", "id").rowsBetween(-2, -1)
w_adjuster_id_reported_at_date_2 = Window.partitionBy("adjuster_id", F.year("reported_at_date")).orderBy(F.col("reported_at_date").desc(), "id")
w_policy_product_prev2_adjuste = Window.partitionBy("policy_product", "prev2_adjuster_level").orderBy("approved_amount_sum_2_prior", "id")
w_status_reported_at_dense_ran = Window.partitionBy("status", "reported_at_dense_rank").orderBy("approved_amount_sum_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_claims = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-01-04").cast("date")) & (F.col("claim_type").isin("medical", "theft", "fire")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

enriched = (
    filtered_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .filter(F.col("p.product").isin("TRAVEL", "HOME", "AUTO"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_date",
        F.col("p.product").alias("policy_product"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_status_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.dense_rank().over(w_status_by_reported_at).alias("reported_at_dense_rank"),
        F.dense_rank().over(w_all_by_approved_amount).alias("approved_amount_dense_rank"),
        F.round(F.avg("adjuster_level").over(w_adjuster_id_reported_at_date), 2).alias("adjuster_level_avg_7"),
        F.nth_value("adjuster_level", 2).over(w_all_by_reported_at_full).alias("second_adjuster_level"),
        F.lead("claim_date").over(w_all_by_adjuster_level).alias("next_claim_date"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_level), 2).alias("approved_amount_vs_adjuster_level_avg"),
        F.sum("approved_amount").over(w_all_by_approved_amount_prior).alias("approved_amount_sum_2_prior"),
        F.lag("adjuster_level", 2).over(w_adjuster_id_reported_at_date_2).alias("prev2_adjuster_level"),
        F.sum("approved_amount").over(w_adjuster_level).alias("adjuster_level_sum_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
    )
)

top_per_group = with_windows.filter(F.col("reported_at_dense_rank") <= 5)

ranked = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "reported_at_date",
        "policy_product",
        "adjuster_name",
        "adjuster_level",
        "rows_in_policy_id",
        "reported_at_dense_rank",
        "approved_amount_dense_rank",
        "adjuster_level_avg_7",
        "second_adjuster_level",
        "approved_amount_percent_rank",
        "approved_amount_sum_2_prior",
        "prev2_adjuster_level",
        "claimed_amount_share_of_policy_id",
        F.row_number().over(w_policy_product_prev2_adjuste).alias("rn"),
        F.ntile(5).over(w_policy_product_prev2_adjuste).alias("approved_amount_sum_2_prior_quintile"),
        (F.col("second_adjuster_level") - F.lag("second_adjuster_level").over(w_policy_product_prev2_adjuste)).alias("second_adjuster_level_change"),
        F.first("claim_date").over(w_status_reported_at_dense_ran).alias("first_claim_date"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "reported_at_date",
        "policy_product",
        "adjuster_name",
        "adjuster_level",
        "rows_in_policy_id",
        "reported_at_dense_rank",
        "approved_amount_dense_rank",
        "second_adjuster_level",
        "approved_amount_percent_rank",
        "prev2_adjuster_level",
        "claimed_amount_share_of_policy_id",
        "rn",
        "approved_amount_sum_2_prior_quintile",
        "second_adjuster_level_change",
        "first_claim_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.length(F.trim(F.col("policy_product"))).alias("policy_product_len"),
    )
    .orderBy(F.col("status_or_unknown").asc(), F.col("status").desc_nulls_first())
)
