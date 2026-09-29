from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

enriched = (
    joined_ranges
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "status",
        "product",
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

derived = (
    enriched
    .select(
        "*",
        F.concat(F.col("status_claim_type_label"), F.lit("#"), F.col("premium_bucket").cast("string")).alias("status_claim_type_label_ref"),
        F.concat(F.col("status"), F.lit("#"), F.col("claim_date_quarter").cast("string")).alias("status_ref"),
        F.when(F.col("status").isin("IN_REVIEW", "OPEN"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
    )
)

labeled = (
    derived
    .select(
        "id",
        "product",
        "claim_date_quarter",
        "status_ref",
        "status_group",
        F.upper(F.trim(F.col("status_ref"))).alias("status_ref_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.substring(F.trim(F.col("status_ref")), 1, 4).alias("status_ref_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "product",
        "claim_date_quarter",
        "status_ref",
        "status_group",
        "status_ref_clean",
        "status_slug",
        "status_ref_prefix",
        F.abs(F.col("claim_date_quarter") - 2).alias("claim_date_quarter_distance"),
        F.length(F.trim(F.col("product"))).alias("product_len"),
    )
    .orderBy(F.col("product").desc_nulls_first())
)
