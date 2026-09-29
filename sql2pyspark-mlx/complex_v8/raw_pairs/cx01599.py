from pyspark.sql import functions as F

joined_ranges = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_parent = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.adjuster_id",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.product").alias("policy_product"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_parent
    .groupBy("adjuster_id")
    .pivot("policy_product", ["LIFE", "HEALTH", "TRAVEL", "HOME"])
    .agg(F.avg("adjuster_level").alias("avg_v"), F.count("adjuster_level").alias("n"))
)
