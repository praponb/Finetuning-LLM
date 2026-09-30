from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter((F.col("premium").between(1132.1, 3465.4)) & (F.col("holder_id").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
        F.concat_ws("|", F.col("status"), F.col("product")).alias("status_product_label"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("LAPSED", "ACTIVE", "CANCELLED")), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_stats = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    with_stats
    .filter(F.col("status_product_label").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        "start_date_month_start",
        "status_product_label",
        "status_ref",
        "claim_count",
    )
)
