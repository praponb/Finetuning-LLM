from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claim_date", "id")
w_status = Window.partitionBy("status")
w_n_status_is_not_null_distinc = Window.partitionBy("n_status_is_not_null", "distinct_policy_ids").orderBy("total_claimed_amount")
w_n_status_is_not_null_distinc_2 = Window.partitionBy("n_status_is_not_null", "distinct_policy_ids").orderBy("total_claimed_amount", "claim_date_year", "status", "policy_id")

base = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("reported_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

sequenced = (
    base
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.max("approved_amount").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_max_approved_amount"),
        F.row_number().over(w_claim_type_adjuster_id_by_cl).alias("seq_no"),
    )
)

rollup_base = (
    sequenced
    .groupBy(F.year("claim_date").alias("claim_date_year"), "status", "policy_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.round(F.sum("seq_no") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("seq_no_to_claimed_amount_ratio"),
        F.count("*").alias("cnt"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.sum(F.when(~F.col("status").isin("PAID", "OPEN"), F.col("seq_no")).otherwise(0)).alias("seq_no_status_not_in_paid_open"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
    .filter(F.col("cnt") >= 3)
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(~F.col("status").isin("PAID", "APPROVED"), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    rollup_base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

labeled = (
    with_counts
    .select(
        "*",
        F.round(F.col("total_claimed_amount") * 1.1, 2).alias("total_claimed_amount_adj"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

windowed = (
    labeled
    .select(
        "*",
        F.round(F.col("claim_status_count") / F.nullif(F.sum("claim_status_count").over(w_status), F.lit(0)), 4).alias("claim_status_count_share_of_status"),
        F.round(F.cume_dist().over(w_n_status_is_not_null_distinc), 3).alias("total_claimed_amount_cume_dist"),
        F.round(F.percent_rank().over(w_n_status_is_not_null_distinc_2), 3).alias("total_claimed_amount_percent_rank"),
    )
)

with_details_policy = (
    windowed.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.claim_date_year",
        "s.status",
        "s.policy_id",
        "s.n_status_is_not_null",
        "s.cnt",
        "s.total_claimed_amount",
        "s.seq_no_status_not_in_paid_open",
        "s.distinct_policy_ids",
        "s.claim_status_count",
        "s.status_len",
        "s.status_prefix",
        "s.claim_status_count_share_of_status",
        "s.total_claimed_amount_cume_dist",
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    with_details_policy
    .select(
        "claim_date_year",
        "status",
        "policy_id",
        "n_status_is_not_null",
        "cnt",
        "total_claimed_amount",
        "seq_no_status_not_in_paid_open",
        "distinct_policy_ids",
        "claim_status_count",
        "status_prefix",
        "claim_status_count_share_of_status",
        "total_claimed_amount_cume_dist",
        "policy_product",
    )
    .orderBy(F.col("policy_id").desc(), F.col("total_claimed_amount").desc())
)
