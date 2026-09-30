from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_status_claim_type_by_claim_d = Window.partitionBy("status", "claim_type").orderBy(F.col("claim_date").desc(), "id")
w_claim_type = Window.partitionBy("claim_type")
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_all_by_claimed_amount_runnin = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_approved_amount_pr = Window.partitionBy("status").orderBy(F.col("approved_amount").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_status_by_distinct_status_in = Window.partitionBy("status").orderBy(F.col("distinct_status_in_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_distinct_policy = Window.partitionBy("policy_id").orderBy("distinct_policy_id_in_adjuster_id_claim_type", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_decile_by_distinc = Window.partitionBy("claim_date_decile").orderBy("distinct_status_in_claim_type", "id")

filtered = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("policy_id").over(w_adjuster_id_claim_type)).alias("distinct_policy_id_in_adjuster_id_claim_type"),
        F.round(F.cume_dist().over(w_status_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.round(F.cume_dist().over(w_status_claim_type_by_claim_d), 3).alias("claim_date_cume_dist"),
        F.sum("approved_amount").over(w_claim_type).alias("claim_type_sum_approved_amount"),
        F.ntile(10).over(w_status_claim_type_by_claim_d).alias("claim_date_decile"),
        F.lead("approved_amount", 3).over(w_status_claim_type_by_claim_d).alias("next3_approved_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_claimed_amount)).alias("approved_amount_change"),
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_claim_type), 2).alias("approved_amount_vs_adjuster_id_claim_type_avg"),
        F.max("approved_amount").over(w_all_by_claimed_amount_runnin).alias("max_approved_amount_so_far"),
        F.max("claimed_amount").over(w_status_by_approved_amount_pr).alias("claimed_amount_max_3_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_claim_type), 2).alias("claimed_amount_vs_adjuster_id_claim_type_avg"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.first("claimed_amount", ignorenulls=True).over(w_status_by_distinct_status_in).alias("first_known_claimed_amount"),
        F.min("next3_approved_amount").over(w_claim_type).alias("claim_type_min_next3_approved_amount"),
        F.nth_value("claimed_amount", 2).over(w_policy_id_by_distinct_policy).alias("second_claimed_amount"),
        F.round(F.avg("claimed_amount_max_3_prior").over(w_claim_type), 2).alias("claim_type_avg_claimed_amount_max_3_prior"),
        F.round(F.cume_dist().over(w_claim_date_decile_by_distinc), 3).alias("distinct_status_in_claim_type_cume_dist"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "distinct_policy_id_in_adjuster_id_claim_type",
        "claim_date_cume_dist",
        "claim_type_sum_approved_amount",
        "claim_date_decile",
        "next3_approved_amount",
        "approved_amount_change",
        "distinct_status_in_claim_type",
        "approved_amount_vs_adjuster_id_claim_type_avg",
        "max_approved_amount_so_far",
        "claimed_amount_max_3_prior",
        "claimed_amount_vs_adjuster_id_claim_type_avg",
        "rows_in_claim_type",
        "first_known_claimed_amount",
        "claim_type_min_next3_approved_amount",
        "claim_type_avg_claimed_amount_max_3_prior",
        "distinct_status_in_claim_type_cume_dist",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
    .orderBy(F.col("status_ref").asc(), "claim_date")
)
