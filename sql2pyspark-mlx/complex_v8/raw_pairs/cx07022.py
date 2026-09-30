from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 2)) & (F.col("status") == "IN_REVIEW"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

claim_rows = (
    base_claims
    .select(
        F.col("status").alias("label"),
        F.col("reported_at").alias("event_time"),
    )
)

claim_side = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select("status", "reported_at")
)

unioned = claim_rows.union(claim_side)

result = (
    unioned
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("label")
)
