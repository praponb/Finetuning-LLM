from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-08-03").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
    )
)

qualified = (
    recent_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product").isin("TRAVEL", "AUTO")), "left_anti")
)

with_children_claims = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

qualified_2 = (
    with_children_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").isNotNull()) & (F.col("c.approved_amount") < 32878.5), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    qualified_2.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_date",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

holder_id_stats = (
    profiled
    .groupBy("holder_id")
    .agg(F.avg("claim_approved_amount").alias("avg_claim_approved_amount_2"))
)

above_average = (
    profiled.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.claim_approved_amount") > F.col("g.avg_claim_approved_amount_2"))
    .select(
        "s.*",
        "g.avg_claim_approved_amount_2",
        F.round(F.col("s.claim_approved_amount") - F.col("g.avg_claim_approved_amount_2"), 2).alias("claim_approved_amount_minus_avg"),
    )
)

tagged = (
    above_average
    .filter(F.col("premium") <= 3532.7)
    .select(
        "*",
        F.date_trunc("year", F.col("first_claim_date")).alias("first_claim_date_year"),
        F.round(F.col("claim_count") / F.nullif(F.col("claim_approved_amount"), F.lit(0)), 3).alias("claim_count_per_claim_approved_amount"),
        F.round(F.col("claim_approved_amount") / F.nullif(F.col("premium"), F.lit(0)), 3).alias("claim_approved_amount_per_premium"),
    )
)

result = (
    tagged
    .select(
        "id",
        "holder_id",
        "premium",
        "claim_id",
        "claim_approved_amount",
        "first_claim_date",
        "claim_count",
        "first_claim_date_year",
        "claim_count_per_claim_approved_amount",
        "claim_approved_amount_per_premium",
    )
    .orderBy(
        F.col("holder_id").desc(),
        F.col("claim_approved_amount").desc_nulls_first(),
    )
)
