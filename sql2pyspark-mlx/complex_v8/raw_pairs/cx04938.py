from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(~F.col("claim_type").isin("medical", "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

qualified = (
    prepared_claims
    .crossJoin(prepared_claims.agg(F.avg("claimed_amount").alias("overall_avg_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_avg_claimed_amount"))
    .drop("overall_avg_claimed_amount")
)

totals = (
    qualified
    .groupBy("status_len", "adjuster_id")
    .agg(
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

filtered = (
    totals
    .crossJoin(totals.agg(F.avg("status_len").alias("overall_avg_status_len")))
    .filter(F.col("status_len") > F.col("overall_avg_status_len"))
    .drop("overall_avg_status_len")
)

with_parent_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(2, 3)), "inner")
    .select("s.*", "a.adjuster_name")
)

annotated = (
    with_parent_adjuster
    .select(
        "status_len",
        "adjuster_id",
        "claim_type_list",
        "adjuster_name",
        F.coalesce(F.col("status_len"), F.lit(0)).alias("status_len_filled"),
    )
)

joined = (
    annotated.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.status_len",
        "s.adjuster_id",
        "s.claim_type_list",
        "s.adjuster_name",
        "s.status_len_filled",
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    joined
    .select(
        "status_len",
        "claim_type_list",
        "adjuster_name",
        "status_len_filled",
        "adjuster_adjuster_name",
        "adjuster_team",
    )
    .orderBy("status_len_filled", F.col("adjuster_adjuster_name").asc())
)
