from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (~F.col("a.team").isin("Health", "Motor")) & (F.col("a.level") > 3), "left_semi")
)

scored = (
    with_activity
    .filter(F.col("greatest_approved_amount_claimed_amount") <= 32500.0)
    .withColumn("reported_at_label_2", F.date_format(F.col("reported_at"), "yyyy-MM"))
)

result = (
    scored
    .groupBy("policy_id")
    .pivot("status", ["IN_REVIEW", "REJECTED", "PAID", "APPROVED"])
    .agg(F.count("claimed_amount"))
    .orderBy("policy_id")
)
