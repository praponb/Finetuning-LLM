from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_group_claim_type = Window.partitionBy("status_group", "claim_type")
w_adjuster_id_by_claim_date_fu = Window.partitionBy("adjuster_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_group = Window.partitionBy("status_group")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_policy_id = Window.partitionBy("policy_id")
w_claim_date_year_by_reported = Window.partitionBy(F.year("claim_date")).orderBy(F.col("reported_at").desc())
w_claim_date_year_by_reported_2 = Window.partitionBy(F.year("claim_date")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 42447.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("status").isin("IN_REVIEW", "PAID"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
    )
)

windowed = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "status_group",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_group_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_status_group_claim_type"),
        F.last("claim_date").over(w_adjuster_id_by_claim_date_fu).alias("last_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_group_claim_type), 2).alias("approved_amount_vs_status_group_claim_type_avg"),
        F.sum("approved_amount").over(w_status_group).alias("status_group_sum_approved_amount"),
        F.ntile(5).over(w_all_by_claim_date).alias("claim_date_quintile"),
        F.size(F.collect_set("policy_id").over(w_status_group_claim_type)).alias("distinct_policy_id_in_status_group_claim_type"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_group), 2).alias("claimed_amount_vs_status_group_avg"),
        F.rank().over(w_claim_date_year_by_reported).alias("reported_at_rank"),
        F.last("approved_amount").over(w_claim_date_year_by_reported_2).alias("last_approved_amount"),
    )
)

enriched = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status_group",
        "last_claim_date",
        "claim_date_quintile",
        "distinct_policy_id_in_status_group_claim_type",
        "claimed_amount_vs_status_group_avg",
        "last_approved_amount",
        F.date_trunc("month", F.col("reported_at")).alias("reported_at_month"),
    )
)

result = (
    enriched
    .filter(F.col("last_claim_date") >= F.lit("2024-07-18").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status_group",
        "last_claim_date",
        "distinct_policy_id_in_status_group_claim_type",
        "claimed_amount_vs_status_group_avg",
        "reported_at_month",
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
    )
    .orderBy(F.col("last_claim_date").asc(), F.col("policy_id").desc_nulls_first())
)
