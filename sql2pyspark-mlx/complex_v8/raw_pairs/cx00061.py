from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_holder_region = Window.partitionBy("policy_product", "holder_region").orderBy("days_since_claim_date", "id")
w_policy_product_adjuster_id = Window.partitionBy("policy_product", "adjuster_id")
w_dedup = Window.partitionBy("policy_product", "adjuster_team").orderBy(F.col("policy_start_date").desc(), F.col("id").desc())

recent = (
    df_claims
    .filter((F.col("claim_type") == "delay") & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        F.when(F.col("status").isin("REJECTED", "PAID"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

joined_policy = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status_group",
        "s.approved_amount_filled",
        "s.days_since_claim_date",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.product").alias("policy_product"),
        F.col("po.risk_score").alias("holder_risk_score"),
        F.col("po.region").alias("holder_region"),
    )
)

windowed = (
    joined_policy
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_product_holder_region), 3).alias("days_since_claim_date_cume_dist"),
        F.round(F.avg("days_since_claim_date").over(w_policy_product_adjuster_id), 2).alias("policy_product_adjuster_id_avg_days_since_claim_"),
        F.lag("claimed_amount").over(w_policy_product_holder_region).alias("prev_claimed_amount"),
    )
)

with_details_adjuster = (
    windowed.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status_group",
        "s.approved_amount_filled",
        "s.policy_start_date",
        "s.policy_product",
        "s.holder_risk_score",
        "s.policy_product_adjuster_id_avg_days_since_claim_",
        "s.prev_claimed_amount",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("delay", "collision", "fire"), 1)).alias("claim_claim_type_count"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("claim_claim_type_count_2"),
    )
)

summarized_rows = (
    with_details_adjuster.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count_2"), F.lit(0)).alias("claim_claim_type_count_2"),
    )
)

unique_rows = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status_group",
        "approved_amount_filled",
        "policy_product",
        "holder_risk_score",
        "policy_product_adjuster_id_avg_days_since_claim_",
        "prev_claimed_amount",
        "adjuster_level",
        "adjuster_team",
        "first_claim_reported_at",
        "claim_claim_type_count_2",
    )
    .orderBy("claim_date", F.col("first_claim_reported_at").asc_nulls_last(), "id")
    .limit(5)
)
