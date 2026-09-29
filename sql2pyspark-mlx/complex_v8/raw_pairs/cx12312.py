from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status = Window.partitionBy("policy_id", "status")
w_policy_id_by_reported_at_plu = Window.partitionBy("policy_id").orderBy("reported_at_plus_2h")
w_policy_id_claim_date_week_mo = Window.partitionBy("policy_id", F.date_trunc("month", "claim_date_week")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter(F.col("claim_type") == "medical")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_policy_id_status), 2).alias("policy_id_status_avg_approved_amount"),
        F.dense_rank().over(w_policy_id_by_reported_at_plu).alias("reported_at_plus_2h_dense_rank"),
        F.min("claimed_amount").over(w_policy_id_claim_date_week_mo).alias("min_claimed_amount_so_far"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["claim_type", "reported_at_plus_2h_dense_rank", "policy_id"], ["claim_type"], ["reported_at_plus_2h_dense_rank"], []],
        "claim_type", "reported_at_plus_2h_dense_rank", "policy_id",
    )
    .agg(
        F.grouping("claim_type").alias("is_total_claim_type"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("claimed_amount"), 2).alias("total_claimed_amount"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("reported_at_plus_2h_dense_rank").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
