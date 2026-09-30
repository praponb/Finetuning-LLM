from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_adjuster_id_by_policy_start = Window.partitionBy("adjuster_id").orderBy(F.col("policy_start_date").desc(), "id")
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy(F.col("approved_amount").asc_nulls_last())
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_adjuster_id_by_policy_start_2 = Window.partitionBy("adjuster_id").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_claims = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
    )
)

with_parent_policy = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

with_trends = (
    with_parent_policy
    .select(
        "*",
        F.count(F.lit(1)).over(w_adjuster_id_policy_id).alias("rows_in_adjuster_id_policy_id"),
        F.dense_rank().over(w_adjuster_id_by_policy_start).alias("policy_start_date_dense_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("approved_amount_vs_adjuster_id_policy_id_avg"),
        F.dense_rank().over(w_status_by_approved_amount_ti).alias("approved_amount_dense_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_status), 2).alias("approved_amount_vs_policy_id_status_avg"),
        F.row_number().over(w_all_by_approved_amount).alias("seq_no"),
        F.first("holder_joined_on").over(w_adjuster_id_by_policy_start_2).alias("first_holder_joined_on"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

result = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "rows_in_adjuster_id_policy_id",
        "policy_start_date_dense_rank",
        "approved_amount_vs_adjuster_id_policy_id_avg",
        "approved_amount_vs_policy_id_status_avg",
        "first_holder_joined_on",
        F.abs(F.col("approved_amount") - 20843.1).alias("approved_amount_distance"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
    .orderBy(
        F.col("approved_amount_vs_adjuster_id_policy_id_avg").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
