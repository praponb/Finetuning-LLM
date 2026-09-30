from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("adjuster_id").orderBy(F.col("claim_type_len").desc(), F.col("id").desc())

filtered = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("reported_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined_policy = (
    latest.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product").isin("LIFE", "HOME", "TRAVEL")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.status_ref",
        "s.claim_type_len",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("approved_amount").alias("min_claim_approved_amount"))
)

summarized_rows = (
    joined_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.min_claim_approved_amount")
)

scored = (
    summarized_rows
    .select(
        "*",
        F.initcap(F.trim(F.col("status_ref"))).alias("status_ref_clean"),
        (F.col("policy_premium") + F.col("approved_amount")).alias("policy_premium_plus_approved_amount"),
    )
)

line_level = (
    scored.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.policy_premium",
        "s.policy_product",
        "s.min_claim_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_date",
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "policy_premium",
        "policy_product",
        "min_claim_approved_amount",
        "claim_status",
        "claim_reported_at",
        "claim_date",
        "claim_approved_amount",
        F.when(F.col("claim_status").isin("OPEN", "IN_REVIEW"), "group_a").when(F.col("claim_status") == "APPROVED", "group_b").otherwise("other").alias("claim_status_group"),
        F.concat_ws("|", F.col("claim_type"), F.col("policy_product")).alias("claim_type_policy_product_label"),
    )
    .orderBy(F.col("min_claim_approved_amount").desc())
)
