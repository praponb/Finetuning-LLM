from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.month("claim_date"))
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy("approved_amount", "id")
w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_claim_da = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("claim_date", "id").rowsBetween(-2, -1)
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy("approved_amount", "id").rowsBetween(-6, Window.currentRow)
w_adjuster_id_policy_id_by_cla = Window.partitionBy("adjuster_id", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_date_month = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date"))
w_policy_id_distinct_policy_id = Window.partitionBy("policy_id", "distinct_policy_id_in_claim_type_adjuster_id")
w_distinct_policy_id_in_claim = Window.partitionBy("distinct_policy_id_in_claim_type_adjuster_id", F.month("first_known_reported_at")).orderBy("claim_type_adjuster_id_max_approved_amount", "id")
w_all_by_claim_date_month_sum = Window.orderBy(F.col("claim_date_month_sum_approved_amount").desc(), "id")

filtered = (
    df_claims
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

ranked = (
    filtered
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_date_month), F.lit(0)), 4).alias("approved_amount_share_of_claim_date_month"),
        F.lag("claimed_amount").over(w_policy_id_by_approved_amount).alias("prev_claimed_amount"),
        F.first("reported_at", ignorenulls=True).over(w_adjuster_id_by_reported_at_r).alias("first_known_reported_at"),
        F.round(F.avg("claimed_amount").over(w_claim_date_month_by_claim_da), 2).alias("claimed_amount_avg_2_prior"),
        F.count(F.lit(1)).over(w_claim_type_adjuster_id).alias("rows_in_claim_type_adjuster_id"),
        F.max("approved_amount").over(w_policy_id_by_approved_amount_2).alias("approved_amount_max_7"),
        F.size(F.collect_set("policy_id").over(w_claim_type_adjuster_id)).alias("distinct_policy_id_in_claim_type_adjuster_id"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_policy_id_by_cla).alias("first_known_claimed_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_date_month), 2).alias("approved_amount_vs_claim_date_month_avg"),
        F.max("approved_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_max_approved_amount"),
        F.sum("approved_amount").over(w_claim_date_month).alias("claim_date_month_sum_approved_amount"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.round(F.col("claimed_amount_avg_2_prior") / F.nullif(F.sum("claimed_amount_avg_2_prior").over(w_policy_id_claim_date_month), F.lit(0)), 4).alias("claimed_amount_avg_2_prior_share_of_policy_id_cl"),
        F.round(F.col("prev_claimed_amount") - F.avg("prev_claimed_amount").over(w_policy_id_distinct_policy_id), 2).alias("prev_claimed_amount_vs_policy_id_distinct_policy"),
        F.min("approved_amount").over(w_policy_id_claim_date_month).alias("policy_id_claim_date_month_min_approved_amount"),
        F.ntile(3).over(w_distinct_policy_id_in_claim).alias("claim_type_adjuster_id_max_approved_amount_terci"),
        F.ntile(4).over(w_all_by_claim_date_month_sum).alias("claim_date_month_sum_approved_amount_quartile"),
    )
)

result = (
    ranked_2
    .filter(F.col("claim_date").between(F.lit("2024-04-09").cast("date"), F.lit("2025-04-25").cast("date")))
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "reported_at",
        "approved_amount_share_of_claim_date_month",
        "claimed_amount_avg_2_prior",
        "rows_in_claim_type_adjuster_id",
        "approved_amount_max_7",
        "distinct_policy_id_in_claim_type_adjuster_id",
        "approved_amount_vs_claim_date_month_avg",
        "claim_type_adjuster_id_max_approved_amount",
        "claim_date_month_sum_approved_amount",
        "claimed_amount_avg_2_prior_share_of_policy_id_cl",
        "prev_claimed_amount_vs_policy_id_distinct_policy",
        "claim_date_month_sum_approved_amount_quartile",
    )
    .orderBy(
        F.col("prev_claimed_amount_vs_policy_id_distinct_policy").desc_nulls_first(),
        "distinct_policy_id_in_claim_type_adjuster_id",
    )
)
