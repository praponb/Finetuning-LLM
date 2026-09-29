from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_policy_product_policy_id = Window.partitionBy("policy_product", "policy_id")

prepared_claims = (
    df_claims
    .filter((F.col("status").isin("OPEN", "PAID", "IN_REVIEW")) & (F.col("adjuster_id") >= 4))
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
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.date_add(F.col("claim_date"), 90).alias("claim_date_plus_90d"),
    )
)

combined_details = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
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
        "s.claim_date_plus_90d",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_all_by_adjuster_level)).alias("adjuster_level_change"),
        F.ntile(5).over(w_all_by_adjuster_level).alias("adjuster_level_quintile"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_policy_product_policy_id), 2).alias("adjuster_level_vs_policy_product_policy_id_avg"),
    )
)

derived = (
    ranked
    .filter(F.col("policy_start_date").isNotNull())
    .select(
        "*",
        F.last_day(F.col("policy_end_date")).alias("policy_end_date_month_end"),
        F.greatest(F.col("approved_amount"), F.col("adjuster_level")).alias("greatest_approved_amount_adjuster_level"),
        F.date_trunc("quarter", F.col("claim_date_plus_90d")).alias("claim_date_plus_90d_quarter"),
    )
)

derived_2 = (
    derived
    .select(
        "*",
        (F.col("adjuster_level_change") - F.col("approved_amount")).alias("adjuster_level_change_minus_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
    )
)

result = (
    derived_2
    .filter(F.col("adjuster_level") < 3)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_date_plus_90d",
        "policy_start_date",
        "policy_end_date",
        "policy_product",
        "adjuster_level",
        "adjuster_level_change",
        "adjuster_level_vs_policy_product_policy_id_avg",
        "greatest_approved_amount_adjuster_level",
        "claim_date_plus_90d_quarter",
        "adjuster_level_change_minus_approved_amount",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy(F.col("claim_date_quarter").desc_nulls_first())
)
