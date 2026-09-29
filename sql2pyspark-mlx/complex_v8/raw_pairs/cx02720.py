from pyspark.sql import functions as F

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

derived = (
    in_window
    .filter(F.col("policy_id") >= 24)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "claim_type",
        "product",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter_2"),
    )
)

combined_details_adjuster = (
    derived.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    combined_details_adjuster
    .groupBy("policy_id")
    .pivot("claim_type", ["theft", "medical"])
    .agg(F.min("adjuster_level"))
    .orderBy("policy_id")
)
