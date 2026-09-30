from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter((F.col("product").isin("TRAVEL", "HEALTH", "HOME")) & (F.col("end_date") >= F.lit("2026-01-24").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.last_day(F.col("start_date")).alias("start_date_month_end"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

detail_rows = (
    scoped.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 30001.6), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        "s.status_prefix",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

claim_adjuster_id_stats = (
    detail_rows
    .groupBy("claim_adjuster_id")
    .agg(F.percentile_approx("claim_claimed_amount", 0.5).alias("median_claim_claimed_amount"))
)

outliers = (
    detail_rows.alias("s")
    .join(claim_adjuster_id_stats.alias("g"), F.col("g.claim_adjuster_id") == F.col("s.claim_adjuster_id"), "inner")
    .filter(F.col("s.claim_claimed_amount") >= F.col("g.median_claim_claimed_amount"))
    .select(
        "s.*",
        "g.median_claim_claimed_amount",
        F.round(F.col("s.claim_claimed_amount") - F.col("g.median_claim_claimed_amount"), 2).alias("claim_claimed_amount_minus_median"),
    )
)

screened = (
    outliers.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.claim_adjuster_id")) & (F.col("c.status").isin("OPEN", "APPROVED", "PAID")), "left_anti")
    .filter(F.col("holder_id") <= 3)
)

result = (
    screened
    .groupBy("start_date", "holder_id")
    .pivot("status", ["ACTIVE", "LAPSED", "CANCELLED", "EXPIRED"])
    .agg(F.sum("median_claim_claimed_amount"))
)
