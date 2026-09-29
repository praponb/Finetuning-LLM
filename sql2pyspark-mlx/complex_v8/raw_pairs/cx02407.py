from pyspark.sql import functions as F

covered = (
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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

summarized_rows = (
    covered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

result = (
    summarized_rows
    .groupingSets(
        [["policy_id", "adjuster_id", "product"], ["policy_id"], ["adjuster_id"]],
        "policy_id", "adjuster_id", "product",
    )
    .agg(
        F.sum(F.when(~F.col("product").isin("LIFE", "TRAVEL"), F.col("claimed_amount"))).alias("claimed_amount_product_not_in_life_travel"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
)
