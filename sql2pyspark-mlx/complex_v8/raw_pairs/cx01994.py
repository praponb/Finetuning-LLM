from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_end_date_year = Window.partitionBy(F.year("policy_end_date"))
w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_minus_max").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_by_claimed_amoun_2 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_minus_max").desc_nulls_first())
w_adjuster_id_by_claimed_amoun_3 = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount_minus_max").desc_nulls_first(), "id")

base_claims = (
    df_claims
    .filter((~F.col("claim_type").isin("medical", "collision")) & (F.col("policy_id") >= 21))
)

with_parent = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product") != "TRAVEL")
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
        F.col("p.product").alias("policy_product"),
    )
)

policy_product_stats = (
    with_parent
    .groupBy("policy_product")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

vs_group = (
    with_parent.alias("s")
    .join(policy_product_stats.alias("g"), F.col("g.policy_product") == F.col("s.policy_product"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    vs_group.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_details_policy = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.policy_product",
        "s.max_claimed_amount",
        "s.claimed_amount_minus_max",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_windows = (
    with_details_policy
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_end_date_year), F.lit(0)), 4).alias("approved_amount_share_of_policy_end_date_year"),
        F.first("claimed_amount", ignorenulls=True).over(w_adjuster_id_by_claimed_amoun).alias("first_known_claimed_amount"),
        F.rank().over(w_adjuster_id_by_claimed_amoun_2).alias("claimed_amount_minus_max_rank"),
        F.ntile(5).over(w_adjuster_id_by_claimed_amoun_3).alias("claimed_amount_minus_max_quintile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "policy_product",
        "claimed_amount_minus_max",
        "policy_end_date",
        "approved_amount_share_of_policy_end_date_year",
        "claimed_amount_minus_max_rank",
        "claimed_amount_minus_max_quintile",
        F.concat_ws(" / ", F.col("status"), F.col("policy_product")).alias("status_policy_product_label"),
    )
    .orderBy(F.col("adjuster_id").asc())
)
