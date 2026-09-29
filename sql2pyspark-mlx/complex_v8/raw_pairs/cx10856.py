from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.col("policy_id").isNotNull()))
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
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.min("claim_date").alias("first_claim_date_2"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_date", "c.first_claim_date_2")
)

per_group = (
    summarized_rows
    .filter(F.col("adjuster_id").isNotNull())
    .groupBy(
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
        F.date_trunc("month", "first_claim_date_2").alias("first_claim_date_2_month"),
        "adjuster_id",
    )
    .agg(
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("claimed_amount"))).alias("claimed_amount_status_open"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
)

result = (
    per_group
    .filter(F.hour(F.col("reported_at_month")).between(6, 11))
    .select(
        "reported_at_month",
        "first_claim_date_2_month",
        "adjuster_id",
        "claim_date_span_days",
        "claimed_amount_status_open",
        "distinct_claim_types",
        F.round((F.unix_timestamp(F.col("reported_at_month")) - F.unix_timestamp(F.col("first_claim_date_2_month"))) / 3600, 1).alias("hours_first_claim_date_2_month_to_reported_at_mo"),
        (F.col("first_claim_date_2_month") + F.expr("INTERVAL 1 DAYS")).alias("first_claim_date_2_month_plus_1d"),
    )
)
