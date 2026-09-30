from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.to_date(F.col("reported_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

derived = (
    cleaned
    .select(
        "*",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
        F.concat_ws(" / ", F.col("status"), F.col("reported_at_label")).alias("status_reported_at_label_label"),
        F.when(F.col("approved_amount") >= 16404.6, "A").when(F.col("approved_amount") >= 4554.8, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

derived_2 = (
    derived
    .filter(F.col("status_reported_at_label_label").isNotNull())
    .select(
        "*",
        (F.col("claim_date_week") + F.expr("INTERVAL 1 DAYS")).alias("claim_date_week_plus_1d"),
        F.date_format(F.col("claim_date_week"), "yyyy-MM-dd HH:00").alias("claim_date_week_label"),
        F.concat_ws(": ", F.col("reported_at_label"), F.col("status")).alias("reported_at_label_status_label"),
    )
)

with_parent = (
    derived_2.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") < 3)
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count("*").alias("claim_count_2"),
        F.min("reported_at").alias("first_claim_reported_at"),
    )
)

profiled = (
    with_parent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
        "c.first_claim_reported_at",
    )
)

result = (
    profiled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "status_prefix",
        "claim_date_week",
        "approved_amount_tier",
        "claim_date_week_label",
        "reported_at_label_status_label",
        "claim_count",
        "claim_count_2",
        "first_claim_reported_at",
        F.dayofweek(F.col("first_claim_reported_at")).alias("first_claim_reported_at_dayofweek"),
    )
    .orderBy("first_claim_reported_at", "approved_amount")
)
