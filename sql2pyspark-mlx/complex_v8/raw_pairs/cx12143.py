from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_reported_at_month_by_c = Window.partitionBy(F.date_trunc("month", "claim_reported_at")).orderBy("claim_claim_date", "id", "claim_id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_claim_claim_date_d = Window.partitionBy("policy_id", "claim_claim_date_decile")

prepared_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 18478.3) & (F.col("policy_id") >= 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
    )
)

line_level = (
    prepared_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

metrics = (
    line_level
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "claim_id",
        "claim_reported_at",
        "claim_approved_amount",
        F.ntile(10).over(w_claim_reported_at_month_by_c).alias("claim_claim_date_decile"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") >= 19609.8) & (F.col("c.status").isNotNull()), "left_semi")
)

sequenced = (
    without_matches
    .withColumn("distinct_adjuster_id_in_policy_id_claim_claim_da", F.size(F.collect_set("adjuster_id").over(w_policy_id_claim_claim_date_d)))
    .withColumn("policy_id_claim_claim_date_decile_sum_approved_a", F.sum("approved_amount").over(w_policy_id_claim_claim_date_d))
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "claim_id",
        "claim_approved_amount",
        "claim_claim_date_decile",
        "approved_amount_vs_adjuster_id_avg",
        "policy_id_claim_claim_date_decile_sum_approved_a",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy(F.col("policy_id").desc())
)
