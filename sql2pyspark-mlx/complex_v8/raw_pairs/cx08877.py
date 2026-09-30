from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product").orderBy("claim_date", "id")

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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

enriched = (
    covered
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.when(F.col("premium") >= 4417.0, "A").when(F.col("premium") >= 1541.8, "B").otherwise("C").alias("premium_tier"),
    )
)

latest_per_key = (
    enriched
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    latest_per_key
    .filter(F.col("policy_id") >= 24)
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "adjuster_id",
        "product",
        "days_since_claim_date",
        "premium_tier",
        F.initcap(F.trim(F.col("premium_tier"))).alias("premium_tier_clean"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

result = (
    derived
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "adjuster_id",
        "product",
        "days_since_claim_date",
        "premium_tier",
        "premium_tier_clean",
        "claim_date_label",
        F.when(F.col("days_since_claim_date") >= 420, "HIGH").when(F.col("days_since_claim_date") >= 210, "MEDIUM").otherwise("LOW").alias("days_since_claim_date_tier"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
    .orderBy(F.col("claim_date_label").asc())
)
