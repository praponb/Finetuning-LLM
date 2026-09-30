from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_product", "policy_id").orderBy(F.col("adjuster_level").desc(), F.col("id").desc())

base = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2024)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

latest_per_key = (
    combined_details
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    latest_per_key
    .filter(F.col("approved_amount_plus_claimed_amount") < 75000.0)
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.round(F.col("approved_amount") / F.nullif(F.col("status_len"), F.lit(0)), 3).alias("approved_amount_per_status_len"),
    )
)

with_parent = (
    labeled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") < 3532.7), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("p.end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.status_clean",
        "s.status_len",
        "s.approved_amount_plus_claimed_amount",
        "s.policy_premium",
        "s.policy_end_date",
        "s.policy_product",
        "s.adjuster_level",
        "s.adjuster_name",
        "s.approved_amount_per_status_len",
        F.col("p.premium").alias("policy_premium_2"),
        F.col("p.end_date").alias("policy_end_date_2"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level_2"),
    )
)

exploded_detail = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount").between(11235.6, 33690.0)), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.status_clean",
        "s.status_len",
        "s.policy_end_date",
        "s.policy_product",
        "s.adjuster_name",
        "s.adjuster_adjuster_name",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    exploded_detail
    .cube("claim_id", "claim_status", "status_len")
    .agg(
        F.array_sort(F.collect_set("adjuster_name")).alias("adjuster_name_list"),
        F.round(F.avg("status_len"), 2).alias("avg_status_len"),
    )
    .select(
        "claim_id",
        F.coalesce(F.col("claim_status"), F.lit("ALL")).alias("claim_status"),
        "status_len",
        "adjuster_name_list",
        "avg_status_len",
    )
    .orderBy(
        F.col("claim_id").asc_nulls_last(),
        F.col("claim_status").asc_nulls_last(),
        F.col("status_len").asc_nulls_last(),
    )
)
