from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_dedup = Window.partitionBy("policy_id").orderBy("max_claimed_amount_adj_so_far", "id")

prepared = (
    df_claims
    .filter((F.col("policy_id") >= 4) & (F.col("claimed_amount") <= 32878.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
    )
)

with_windows = (
    prepared
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status_claim_type_label",
        "claimed_amount_adj",
        F.max("claimed_amount_adj").over(w_all_by_claim_date_running).alias("max_claimed_amount_adj_so_far"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id_adjuster_id), 2).alias("approved_amount_vs_policy_id_adjuster_id_avg"),
        F.last("approved_amount", ignorenulls=True).over(w_all_by_claim_date_running).alias("last_known_approved_amount"),
        F.size(F.collect_set("status").over(w_policy_id_adjuster_id)).alias("distinct_status_in_policy_id_adjuster_id"),
        F.sum("claimed_amount_adj").over(w_all_by_claim_date_running).alias("running_claimed_amount_adj"),
    )
)

without_matches = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.hour(F.col("c.reported_at")).between(6, 12)), "left_semi")
)

latest_per_key = (
    without_matches
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status_claim_type_label",
        "claimed_amount_adj",
        "max_claimed_amount_adj_so_far",
        "approved_amount_vs_policy_id_adjuster_id_avg",
        "distinct_status_in_policy_id_adjuster_id",
        "running_claimed_amount_adj",
    )
    .orderBy(F.col("status_claim_type_label").desc(), "id")
    .limit(5)
)
