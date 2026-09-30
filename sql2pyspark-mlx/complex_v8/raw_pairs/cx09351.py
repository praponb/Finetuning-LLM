from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_date_year_by = Window.partitionBy("policy_id", F.year("claim_date")).orderBy("approved_amount", "id").rowsBetween(-2, -1)
w_policy_id_claim_date_year_by_2 = Window.partitionBy("policy_id", F.year("claim_date")).orderBy("approved_amount", "id")

valid = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.col("reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
        F.when(F.col("claimed_amount") >= 34763.4, "A").when(F.col("claimed_amount") >= 10518.4, "B").otherwise("C").alias("claimed_amount_tier"),
    )
)

with_parent_policy = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        "po.holder_name",
        F.col("po.region").alias("holder_region"),
    )
)

scored = (
    with_parent_policy
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at_label",
        "claimed_amount_tier",
        "holder_name",
        "holder_region",
        (F.col("policy_premium") - F.col("approved_amount")).alias("policy_premium_minus_approved_amount"),
        F.concat(F.col("holder_name"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("holder_name_ref"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

windowed = (
    scored
    .withColumn("approved_amount_avg_2_prior", F.round(F.avg("approved_amount").over(w_policy_id_claim_date_year_by), 2))
    .withColumn("approved_amount_rank", F.rank().over(w_policy_id_claim_date_year_by_2))
)

exploded_detail_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.reported_at_label",
        "s.claimed_amount_tier",
        "s.holder_name",
        "s.policy_premium_minus_approved_amount",
        "s.holder_name_ref",
        "s.approved_amount_filled",
        "s.approved_amount_avg_2_prior",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    exploded_detail_claims
    .select(
        "id",
        "claim_id",
        "approved_amount_filled",
        "policy_premium_minus_approved_amount",
        "claim_claimed_amount",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["approved_amount_filled", "policy_premium_minus_approved_amount", "claim_claimed_amount"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
