from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region_by_joined_on_full = Window.partitionBy("region").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_region_by_joined_on_running = Window.partitionBy("region").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_region_by_joined_on_ties = Window.partitionBy("region").orderBy("joined_on")

recent_holders = (
    df_policyholders
    .filter(F.col("risk_score") >= 0.1)
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)

screened = (
    recent_holders.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left_semi")
)

ranked = (
    screened
    .select(
        "id",
        F.nth_value("joined_on", 2).over(w_region_by_joined_on_full).alias("second_joined_on"),
        F.last("joined_on", ignorenulls=True).over(w_region_by_joined_on_running).alias("last_known_joined_on"),
        F.round(F.cume_dist().over(w_region_by_joined_on_ties), 3).alias("joined_on_cume_dist"),
        F.first("joined_on").over(w_region_by_joined_on_full).alias("first_joined_on"),
    )
)

detail_rows_policys = (
    ranked.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.last_known_joined_on",
        "s.first_joined_on",
        F.col("p.id").alias("policy_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

with_stats = (
    detail_rows_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

filtered = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp")), "left_semi")
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.max("end_date").alias("last_policy_end_date"),
        F.min("premium").alias("min_policy_premium"),
    )
)

with_stats_2 = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        "p.last_policy_end_date",
        "p.min_policy_premium",
    )
)

result = (
    with_stats_2
    .select(
        "id",
        "last_known_joined_on",
        "first_joined_on",
        "policy_id",
        "policy_end_date",
        "policy_product",
        "avg_policy_premium",
        "last_policy_end_date",
        "min_policy_premium",
    )
    .orderBy("policy_product", F.col("avg_policy_premium").desc())
)
