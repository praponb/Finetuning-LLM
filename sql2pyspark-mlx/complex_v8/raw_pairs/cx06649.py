from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_parent = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium") < 3532.7)
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.approved_amount",
        "s.premium",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

screened = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") < 26237.1), "left_anti")
    .filter(F.col("status") == "APPROVED")
)

labeled = (
    screened
    .select(
        "*",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.when(F.col("status").isin("IN_REVIEW", "PAID"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        F.abs(F.col("claimed_amount") - 20436.5).alias("claimed_amount_distance"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "policy_start_date",
        "status_len",
        "status_group",
        "claimed_amount_distance",
        (F.col("claimed_amount") + F.col("status_len")).alias("claimed_amount_plus_status_len"),
    )
    .orderBy(F.col("claimed_amount_distance").asc_nulls_last(), "id")
    .limit(20)
)
