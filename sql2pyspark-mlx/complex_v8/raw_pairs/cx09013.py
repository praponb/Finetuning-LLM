from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_moving = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_status_by_claimed_amount_pri = Window.partitionBy("status").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-2, -1)
w_status_policy_id = Window.partitionBy("status", "policy_id")

recent_claims = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")) & (F.col("claimed_amount") <= 29336.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

joined = (
    recent_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claim_type_ref",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_all_by_claimed_amount_moving), 2).alias("claimed_amount_avg_7"),
        F.round(F.avg("claimed_amount").over(w_status_by_claimed_amount_pri), 2).alias("claimed_amount_avg_2_prior"),
        F.sum("claimed_amount").over(w_status_policy_id).alias("status_policy_id_sum_claimed_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_status_policy_id"),
    )
)

exploded_detail_claims = (
    sequenced.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.claim_type_ref",
        "s.policy_end_date",
        "s.status_policy_id_sum_claimed_amount",
        "s.claimed_amount_share_of_status_policy_id",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

labeled = (
    exploded_detail_claims
    .select(
        "*",
        (F.col("claim_claimed_amount") - F.col("claimed_amount")).alias("claim_claimed_amount_minus_claimed_amount"),
    )
)

result = (
    labeled
    .filter(F.col("claim_claimed_amount") > 19347.2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "claim_type_ref",
        "status_policy_id_sum_claimed_amount",
        "claimed_amount_share_of_status_policy_id",
        "claim_id",
        "claim_status",
        "claim_claimed_amount_minus_claimed_amount",
        F.quarter(F.col("policy_end_date")).alias("policy_end_date_quarter"),
        F.date_format(F.col("policy_end_date"), "MMM yyyy").alias("policy_end_date_label"),
    )
)
