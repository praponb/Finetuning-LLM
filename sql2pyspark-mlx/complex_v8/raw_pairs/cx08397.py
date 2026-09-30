from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claimed_amount_adj = Window.partitionBy("status").orderBy(F.col("claimed_amount_adj").desc(), "id", "claim_id").rowsBetween(-3, -1)
w_policy_id = Window.partitionBy("policy_id")
w_status_by_claimed_amount_adj_2 = Window.partitionBy("status").orderBy(F.col("claimed_amount_adj").desc(), "id", "claim_id")

filtered_claims = (
    df_claims
    .filter((F.col("approved_amount") <= 32878.5) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        F.when(F.col("approved_amount") >= 26237.1, "HIGH").when(F.col("approved_amount") >= 1364.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

with_children = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") < F.lit("2025-01-04").cast("date")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.claimed_amount_adj",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

ranked = (
    with_children
    .select(
        "*",
        F.sum("claimed_amount").over(w_status_by_claimed_amount_adj).alias("claimed_amount_sum_3_prior"),
        F.round(F.avg("claimed_amount").over(w_policy_id), 2).alias("policy_id_avg_claimed_amount"),
        F.row_number().over(w_status_by_claimed_amount_adj_2).alias("rn"),
        F.datediff(F.col("claim_claim_date"), F.lag("claim_claim_date").over(w_status_by_claimed_amount_adj_2)).alias("days_since_prev_claim_claim_date"),
    )
)

summary = (
    ranked
    .groupBy(
        "claim_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.sum("claim_claimed_amount").alias("total_claim_claimed_amount"),
        F.count("*").alias("cnt"),
    )
)

with_details_claim = (
    summary.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claim_type").isNotNull()), "left")
    .select(
        "s.*",
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
    )
)

result = (
    with_details_claim
    .select(
        "claim_id",
        "claim_date_ym",
        "total_claim_claimed_amount",
        "cnt",
        "claim_date",
        F.initcap(F.trim(F.col("claim_status"))).alias("claim_status_clean"),
    )
    .orderBy(F.col("claim_id").desc())
)
