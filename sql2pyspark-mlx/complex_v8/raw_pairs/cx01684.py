from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc(), "id")
w_claim_type_policy_start_date = Window.partitionBy("claim_type", F.date_trunc("month", "policy_start_date")).orderBy("policy_start_date", "id")
w_claim_type_policy_start_date_2 = Window.partitionBy("claim_type", F.date_trunc("month", "policy_start_date")).orderBy("policy_start_date")
w_claim_type_policy_start_date_3 = Window.partitionBy("claim_type", F.date_trunc("month", "policy_start_date")).orderBy("policy_start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_policy_start_date_tie = Window.orderBy(F.col("policy_start_date").desc())
w_claim_type = Window.partitionBy("claim_type")
w_policy_start_date_month_poli = Window.partitionBy(F.date_trunc("month", "policy_start_date"), "policy_holder_id")
w_status_by_policy_start_date = Window.partitionBy("status").orderBy("policy_start_date")
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

joined_policy = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_trends = (
    joined_policy
    .select(
        "*",
        F.row_number().over(w_policy_id_by_approved_amount).alias("seq_no"),
        F.row_number().over(w_claim_type_policy_start_date).alias("policy_start_date_seq"),
        F.round(F.cume_dist().over(w_claim_type_policy_start_date_2), 3).alias("policy_start_date_cume_dist"),
        F.count(F.lit(1)).over(w_claim_type_policy_start_date_3).alias("running_count"),
        F.dense_rank().over(w_claim_type_policy_start_date_2).alias("policy_start_date_dense_rank"),
        F.dense_rank().over(w_all_by_policy_start_date_tie).alias("policy_start_date_dense_rank_2"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type), 2).alias("approved_amount_vs_claim_type_avg"),
        F.rank().over(w_all_by_policy_start_date_tie).alias("policy_start_date_rank"),
        F.size(F.collect_set("adjuster_id").over(w_policy_start_date_month_poli)).alias("distinct_adjuster_id_in_policy_start_date_month_"),
        F.rank().over(w_status_by_policy_start_date).alias("policy_start_date_rank_2"),
        F.max("approved_amount").over(w_all_by_approved_amount_runni).alias("max_approved_amount_so_far"),
    )
)

result = (
    with_trends
    .filter(F.col("policy_start_date_seq").between(4, 9))
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "status",
        "policy_holder_id",
        "policy_start_date",
        "seq_no",
        "policy_start_date_seq",
        "policy_start_date_cume_dist",
        "running_count",
        "policy_start_date_dense_rank",
        "approved_amount_vs_claim_type_avg",
        "policy_start_date_rank_2",
        "max_approved_amount_so_far",
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
    )
    .orderBy(F.col("status").asc_nulls_last(), "id")
    .limit(10)
)
