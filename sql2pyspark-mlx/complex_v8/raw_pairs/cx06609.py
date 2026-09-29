from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(-6, -1)
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").asc_nulls_last())
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc(), "id")
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy(F.col("claim_date").desc())
w_latest = Window.partitionBy("claim_type", "status").orderBy(F.col("policy_end_date").desc(), F.col("id").desc())

filtered_claims = (
    df_claims
    .filter((F.col("status").isin("PAID", "APPROVED")) & (F.col("policy_id") >= 13))
)

metrics = (
    filtered_claims
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
        F.round(F.avg("claimed_amount").over(w_policy_id_by_claimed_amount), 2).alias("claimed_amount_avg_6_prior"),
        F.dense_rank().over(w_all_by_approved_amount_ties).alias("approved_amount_dense_rank"),
        F.lag("claimed_amount").over(w_all_by_reported_at).alias("prev_claimed_amount"),
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
        F.max("approved_amount").over(w_policy_id_by_claimed_amount_2).alias("max_approved_amount_so_far"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (~F.col("c.status").isin("APPROVED", "PAID")) & (F.col("c.claim_date").between(F.lit("2024-03-16").cast("date"), F.lit("2025-04-20").cast("date"))), "left_semi")
    .filter(F.col("approved_amount_dense_rank") > 3)
)

with_trends = (
    without_matches
    .select(
        "*",
        F.sum("prev_claimed_amount").over(w_policy_id_by_claim_date_runn).alias("running_prev_claimed_amount"),
        F.size(F.collect_set("adjuster_id").over(w_status_claim_type)).alias("distinct_adjuster_id_in_status_claim_type"),
        F.round(F.percent_rank().over(w_policy_id_by_claim_date), 3).alias("claim_date_percent_rank"),
        F.rank().over(w_policy_id_by_claim_date_ties).alias("claim_date_rank"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_avg_6_prior",
        "s.approved_amount_dense_rank",
        "s.prev_claimed_amount",
        "s.max_approved_amount_so_far",
        "s.running_prev_claimed_amount",
        "s.distinct_adjuster_id_in_status_claim_type",
        "s.claim_date_rank",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

latest_per_key = (
    joined
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "claimed_amount_avg_6_prior",
        "approved_amount_dense_rank",
        "prev_claimed_amount",
        "max_approved_amount_so_far",
        "distinct_adjuster_id_in_status_claim_type",
        "claim_date_rank",
        "policy_end_date",
        "policy_product",
        F.datediff(F.col("claim_date"), F.col("policy_end_date")).alias("days_policy_end_date_to_claim_date"),
    )
    .orderBy("reported_at")
)
