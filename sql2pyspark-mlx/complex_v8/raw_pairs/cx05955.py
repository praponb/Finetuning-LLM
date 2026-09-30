from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())
w_policy_id_by_reported_at = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id")

filtered_claims = (
    df_claims
    .filter(F.col("claimed_amount").between(7204.1, 38909.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.when(F.col("claimed_amount") >= 32878.5, "A").when(F.col("claimed_amount") >= 19347.2, "B").otherwise("C").alias("claimed_amount_tier"),
    )
)

sequenced = (
    filtered_claims
    .select(
        "*",
        F.round(F.col("claimed_amount_filled") - F.avg("claimed_amount_filled").over(w_policy_id), 2).alias("claimed_amount_filled_vs_policy_id_avg"),
        F.round(F.percent_rank().over(w_policy_id_by_reported_at_tie), 3).alias("reported_at_percent_rank"),
        F.ntile(4).over(w_policy_id_by_reported_at).alias("reported_at_quartile"),
        F.dense_rank().over(w_policy_id_by_reported_at).alias("reported_at_dense_rank"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claimed_amount_filled",
        "s.claimed_amount_tier",
        "s.claimed_amount_filled_vs_policy_id_avg",
        "s.reported_at_percent_rank",
        "s.reported_at_quartile",
        "s.reported_at_dense_rank",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_details_policy = (
    with_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.end_date").alias("policy_end_date_2"))
)

combined_details_policy = (
    with_details_policy.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") > 2013.8)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.claimed_amount_filled",
        "s.claimed_amount_tier",
        "s.claimed_amount_filled_vs_policy_id_avg",
        "s.reported_at_percent_rank",
        "s.reported_at_quartile",
        "s.adjuster_name",
        "s.adjuster_team",
        "s.adjuster_level",
        "s.policy_start_date",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
    )
)

derived = (
    combined_details_policy
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount_filled",
        "claimed_amount_tier",
        "claimed_amount_filled_vs_policy_id_avg",
        "reported_at_percent_rank",
        "reported_at_quartile",
        "adjuster_level",
        "policy_product",
        "policy_premium",
        F.lower(F.regexp_replace(F.trim(F.col("policy_product")), " ", "_")).alias("policy_product_slug"),
    )
)

result = (
    derived
    .groupBy("claimed_amount_tier", "reported_at_quartile")
    .pivot("policy_product", ["LIFE", "TRAVEL"])
    .agg(F.max("policy_premium"))
    .orderBy("claimed_amount_tier", "reported_at_quartile")
)
