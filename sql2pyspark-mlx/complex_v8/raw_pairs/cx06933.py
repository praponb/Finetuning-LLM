from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_reported_at_to_date_b = Window.partitionBy("status", F.to_date("reported_at")).orderBy(F.col("approved_amount").desc_nulls_first(), "id")
w_status_policy_status = Window.partitionBy("status", "policy_status")
w_status = Window.partitionBy("status")
w_all_by_policy_end_date_full = Window.orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_end_date_year_policy = Window.partitionBy(F.year("policy_end_date"), "policy_id").orderBy("policy_end_date", "id")
w_status_by_claimed_amount_run = Window.partitionBy("status").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_claimed_amount_ful = Window.partitionBy("status").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_policy_end_date = Window.orderBy("policy_end_date", "id")
w_policy_id_distinct_policy_id = Window.partitionBy("policy_id", "distinct_policy_id_in_status")
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_claims.filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_type").isNotNull()))

combined_details_policy = (
    prepared.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

windowed = (
    combined_details_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "policy_premium",
        "policy_status",
        "policy_end_date",
        F.rank().over(w_status_reported_at_to_date_b).alias("approved_amount_rank"),
        F.min("policy_premium").over(w_status_policy_status).alias("status_policy_status_min_policy_premium"),
        F.min("policy_premium").over(w_status).alias("status_min_policy_premium"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_policy_status), F.lit(0)), 4).alias("claimed_amount_share_of_status_policy_status"),
        F.size(F.collect_set("policy_id").over(w_status)).alias("distinct_policy_id_in_status"),
        F.nth_value("claimed_amount", 2).over(w_all_by_policy_end_date_full).alias("second_claimed_amount"),
        (F.col("policy_premium") - F.lag("policy_premium").over(w_policy_end_date_year_policy)).alias("policy_premium_change"),
        F.lead("policy_premium").over(w_status_reported_at_to_date_b).alias("next_policy_premium"),
        F.last("reported_at", ignorenulls=True).over(w_status_by_claimed_amount_run).alias("last_known_reported_at"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.nth_value("approved_amount", 2).over(w_status_by_claimed_amount_ful).alias("second_approved_amount"),
        F.lag("claimed_amount", 2).over(w_all_by_policy_end_date).alias("prev2_claimed_amount"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_premium",
        "approved_amount_rank",
        "status_policy_status_min_policy_premium",
        "claimed_amount_share_of_status_policy_status",
        "policy_premium_change",
        "next_policy_premium",
        "last_known_reported_at",
        "distinct_adjuster_id_in_status",
        "second_approved_amount",
        "prev2_claimed_amount",
        F.size(F.collect_set("policy_status").over(w_policy_id_distinct_policy_id)).alias("distinct_policy_status_in_policy_id_distinct_pol"),
        F.count(F.lit(1)).over(w_all_by_approved_amount_runni).alias("running_count"),
        F.round(F.avg("next_policy_premium").over(w_policy_id_distinct_policy_id), 2).alias("policy_id_distinct_policy_id_in_status_avg_next_"),
        F.size(F.collect_set("status").over(w_policy_id_distinct_policy_id)).alias("distinct_status_in_policy_id_distinct_policy_id_"),
    )
)

result = (
    with_windows
    .filter(F.col("second_approved_amount") < 26237.1)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "policy_premium",
        "approved_amount_rank",
        "status_policy_status_min_policy_premium",
        "claimed_amount_share_of_status_policy_status",
        "policy_premium_change",
        "next_policy_premium",
        "last_known_reported_at",
        "distinct_adjuster_id_in_status",
        "second_approved_amount",
        "prev2_claimed_amount",
        "distinct_policy_status_in_policy_id_distinct_pol",
        "running_count",
        "policy_id_distinct_policy_id_in_status_avg_next_",
        "distinct_status_in_policy_id_distinct_policy_id_",
    )
)
