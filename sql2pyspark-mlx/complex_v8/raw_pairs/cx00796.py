from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_team", "adjuster_id").orderBy(F.col("reported_at").desc(), F.col("id").desc())
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-02-06").cast("date"))
)

with_parent = (
    with_activity.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.length(F.trim(F.col("a.adjuster_name"))) >= 4)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.status_clean",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

latest = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

windowed = (
    latest
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "adjuster_name",
        "adjuster_team",
        F.sum("claimed_amount").over(w_policy_id_adjuster_id).alias("policy_id_adjuster_id_sum_claimed_amount"),
        F.ntile(10).over(w_all_by_claimed_amount).alias("claimed_amount_decile"),
        F.dense_rank().over(w_all_by_claimed_amount).alias("claimed_amount_dense_rank"),
        F.lead("claimed_amount").over(w_all_by_claimed_amount).alias("next_claimed_amount"),
    )
)

detail_rows_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 28900.9), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.adjuster_team",
        "s.claimed_amount_decile",
        "s.claimed_amount_dense_rank",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

with_activity_2 = (
    detail_rows_claims
    .crossJoin(detail_rows_claims.agg(F.avg("claim_claimed_amount").alias("overall_avg_claim_claimed_amount")))
    .filter(F.col("claim_claimed_amount") > F.col("overall_avg_claim_claimed_amount"))
    .drop("overall_avg_claim_claimed_amount")
)

result = (
    with_activity_2
    .groupBy("policy_id")
    .pivot("adjuster_team", ["Health", "Motor", "Travel"])
    .agg(F.avg("claim_approved_amount"))
    .orderBy("policy_id")
)
