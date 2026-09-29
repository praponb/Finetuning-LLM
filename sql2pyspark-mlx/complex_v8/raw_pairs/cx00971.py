from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2025-01-24").cast("date")) | (F.col("end_date") < F.lit("2026-11-25").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

with_children = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status_prefix",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date") >= F.lit("2025-01-24").cast("date")), "left_anti")
)

tagged = (
    qualified
    .filter(F.col("claim_claimed_amount") > 12010.5)
    .select(
        "id",
        "start_date",
        "status_prefix",
        "claim_id",
        "claim_date",
        "claim_claimed_amount",
        "claim_status",
        "claim_reported_at",
        F.when(F.col("claim_status").isin("REJECTED", "PAID"), "group_a").when(F.col("claim_status") == "OPEN", "group_b").otherwise("other").alias("claim_status_group"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

without_matches = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (F.col("c.claim_date") < F.lit("2024-12-30").cast("date")), "left_anti")
)

scored = (
    without_matches
    .filter(F.col("claim_date_year").between(2024, 2026))
    .select(
        "id",
        "start_date",
        "status_prefix",
        "claim_id",
        "claim_date",
        "claim_status",
        "claim_reported_at",
        "claim_status_group",
        "claim_date_year",
        F.coalesce(F.col("claim_status"), F.lit("UNKNOWN")).alias("claim_status_or_unknown"),
        F.concat(F.col("claim_status_group"), F.lit("#"), F.col("claim_date_year").cast("string")).alias("claim_status_group_ref"),
    )
)

with_details = (
    scored.alias("s")
    .join(df_claims.alias("c"), (F.col("c.id") == F.col("s.claim_id")) & (~F.col("c.status").isin("APPROVED", "PAID")), "left")
    .select("s.*", F.col("c.claimed_amount").alias("claim_claimed_amount"))
)

result = (
    with_details
    .select(
        "id",
        "start_date",
        "status_prefix",
        "claim_id",
        "claim_status",
        "claim_reported_at",
        "claim_status_group",
        "claim_date_year",
        "claim_status_group_ref",
        "claim_claimed_amount",
        F.concat_ws(" / ", F.col("claim_status_group_ref"), F.col("status_prefix")).alias("claim_status_group_ref_status_prefix_label"),
    )
)
