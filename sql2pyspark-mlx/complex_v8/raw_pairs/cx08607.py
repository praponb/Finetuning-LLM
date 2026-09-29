from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_policy_holder_id = Window.partitionBy("policy_holder_id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_policy_holder_id_status_by_s = Window.partitionBy("policy_holder_id", "status").orderBy(F.col("status_sum_claimed_amount").desc())
w_policy_id_next_claim_date_mo = Window.partitionBy("policy_id", F.date_trunc("month", "next_claim_date")).orderBy(F.col("status_sum_claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")

prepared = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-06-13").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

combined_details_policy = (
    prepared.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "EXPIRED"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

windowed = (
    combined_details_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "policy_holder_id",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status), F.lit(0)), 4).alias("claimed_amount_share_of_status"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_holder_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_holder_id"),
        F.count(F.lit(1)).over(w_policy_holder_id).alias("rows_in_policy_holder_id"),
        F.sum("claimed_amount").over(w_status).alias("status_sum_claimed_amount"),
        F.round(F.cume_dist().over(w_all_by_claim_date_ties), 3).alias("claim_date_cume_dist"),
        F.lead("claim_date").over(w_all_by_claim_date).alias("next_claim_date"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
        F.sum("claimed_amount").over(w_policy_holder_id).alias("policy_holder_id_sum_claimed_amount"),
        F.min("claimed_amount").over(w_policy_holder_id).alias("policy_holder_id_min_claimed_amount"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.round(F.percent_rank().over(w_policy_holder_id_status_by_s), 3).alias("status_sum_claimed_amount_percent_rank"),
        F.min("claimed_amount_vs_status_avg").over(w_policy_id_next_claim_date_mo).alias("min_claimed_amount_vs_status_avg_so_far"),
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "status",
        "policy_holder_id",
        "claimed_amount_share_of_status",
        "claimed_amount_vs_status_avg",
        "claimed_amount_share_of_policy_holder_id",
        "rows_in_policy_holder_id",
        "status_sum_claimed_amount",
        "claim_date_cume_dist",
        "next_claim_date",
        "policy_holder_id_sum_claimed_amount",
        "policy_holder_id_min_claimed_amount",
        "status_sum_claimed_amount_percent_rank",
        "policy_id_sum_claimed_amount",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(
        F.col("claimed_amount_vs_status_avg").desc(),
        F.col("next_claim_date").desc(),
        "id",
    )
    .limit(10)
)
