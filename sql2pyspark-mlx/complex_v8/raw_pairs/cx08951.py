from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_claim_type_by_policy_start_d = Window.partitionBy("claim_type").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_reported_at_pri = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-6, -1)
w_claim_type_by_policy_start_d_2 = Window.partitionBy("claim_type").orderBy(F.col("policy_start_date").desc(), "id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_adjuster_id_status_by_claime = Window.partitionBy("adjuster_id", "status").orderBy("claimed_amount", "id")
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")
w_claim_type_by_policy_start_d_3 = Window.partitionBy("claim_type").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_moving = Window.orderBy("claimed_amount", "id").rowsBetween(-1, Window.currentRow)

recent = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2025)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 9608.4).alias("approved_amount_distance"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

enriched_policy = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product").isin("HEALTH", "AUTO", "HOME"))
    .select(
        "s.*",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

metrics = (
    enriched_policy
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.first("claimed_amount").over(w_claim_type_by_policy_start_d).alias("first_claimed_amount"),
        F.count(F.lit(1)).over(w_all_by_claimed_amount_runnin).alias("running_count"),
        F.round(F.col("approved_amount_distance") - F.avg("approved_amount_distance").over(w_status), 2).alias("approved_amount_distance_vs_status_avg"),
        F.round(F.avg("policy_premium").over(w_policy_id_by_reported_at_pri), 2).alias("policy_premium_avg_6_prior"),
        F.row_number().over(w_claim_type_by_policy_start_d_2).alias("seq_no"),
        F.last("approved_amount_distance", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_approved_amount_distance"),
        F.lag("policy_start_date", 3).over(w_all_by_claimed_amount).alias("prev3_policy_start_date"),
        F.row_number().over(w_all_by_claimed_amount).alias("rn"),
        F.row_number().over(w_adjuster_id_status_by_claime).alias("rn_2"),
        F.rank().over(w_policy_id_by_reported_at).alias("reported_at_rank"),
        F.count(F.lit(1)).over(w_claim_type_by_policy_start_d_3).alias("running_count_2"),
        F.sum("approved_amount_distance").over(w_all_by_claimed_amount_moving).alias("approved_amount_distance_sum_2"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
    )
)

top_per_group = metrics.filter((F.col("rn_2") <= 5) & (F.col("policy_id").isNotNull()))

result = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_distance",
        "status_ref",
        "policy_product",
        "policy_start_date",
        "policy_premium",
        "rows_in_status",
        "first_claimed_amount",
        "running_count",
        "approved_amount_distance_vs_status_avg",
        "policy_premium_avg_6_prior",
        "last_known_approved_amount_distance",
        "prev3_policy_start_date",
        "rn",
        "rn_2",
        "running_count_2",
        "approved_amount_distance_sum_2",
        "claimed_amount_share_of_status",
        F.when(F.col("status").isin("OPEN", "PAID"), "group_a").when(F.col("status") == "APPROVED", "group_b").otherwise("other").alias("status_group"),
        F.greatest(F.col("running_count_2"), F.col("rn")).alias("greatest_running_count_2_rn"),
    )
    .orderBy("greatest_running_count_2_rn")
)
