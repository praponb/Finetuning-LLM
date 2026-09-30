from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("approved_amount") < 23207.7) & (F.col("status").isin("IN_REVIEW", "OPEN", "PAID")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

labeled = (
    valid_claims
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "status",
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        F.length(F.trim(F.col("claim_type_slug"))).alias("claim_type_slug_len"),
    )
)

annotated = (
    labeled
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "greatest_claimed_amount_approved_amount",
        "claim_type_slug_len",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

labeled_2 = (
    annotated
    .select(
        "id",
        "claimed_amount",
        "status_or_unknown",
        "status_ref",
        F.when(F.col("status_or_unknown").isin("REJECTED", "APPROVED"), "group_a").when(F.col("status_or_unknown") == "OPEN", "group_b").otherwise("other").alias("status_or_unknown_group"),
    )
)

without_matches = (
    labeled_2
    .crossJoin(labeled_2.agg(F.avg("claimed_amount").alias("overall_avg_claimed_amount")))
    .filter(F.col("claimed_amount") > F.col("overall_avg_claimed_amount"))
    .drop("overall_avg_claimed_amount")
)

derived = (
    without_matches
    .filter(F.col("status_or_unknown_group").isNotNull())
    .withColumn("claimed_amount_bucket", F.floor(F.col("claimed_amount") / 10000) * 10000)
)

tagged = (
    derived
    .select(
        "*",
        F.greatest(F.col("claimed_amount_bucket"), F.col("claimed_amount")).alias("greatest_claimed_amount_bucket_claimed_amount"),
        F.when(F.col("claimed_amount") >= 28900.9, "large").when(F.col("claimed_amount") >= 18478.3, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

result = (
    tagged
    .filter(F.col("claimed_amount") >= 4868.7)
    .select(
        "id",
        "status_or_unknown",
        "status_ref",
        "greatest_claimed_amount_bucket_claimed_amount",
        "claimed_amount_tier",
        F.coalesce(F.col("claimed_amount_tier"), F.lit("UNKNOWN")).alias("claimed_amount_tier_or_unknown"),
    )
    .orderBy(
        F.col("claimed_amount_tier").asc(),
        F.col("greatest_claimed_amount_bucket_claimed_amount").desc(),
    )
)
