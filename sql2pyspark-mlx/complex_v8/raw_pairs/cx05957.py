from pyspark.sql import functions as F

cleaned_policys = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

with_activity = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.upper(F.trim(F.col("p.holder_name"))).like("N%")), "left_semi")
)

filtered = (
    with_activity.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.product") == "LIFE") & (F.col("p.start_date") < F.lit("2025-07-07").cast("date")), "left_semi")
)

derived = (
    filtered
    .filter(F.col("premium") < 4678.8)
    .select(
        "*",
        F.upper(F.trim(F.col("product"))).alias("product_clean"),
        F.concat(F.col("product"), F.lit("#"), F.col("holder_id").cast("string")).alias("product_ref"),
    )
)

with_parent = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", "p.holder_name", F.col("p.segment").alias("holder_segment"))
)

screened = (
    with_parent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0) & (F.col("c.claim_type").isin("fire", "flood", "medical")), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "product_clean",
        "product_ref",
        "holder_segment",
        F.substring(F.trim(F.col("holder_name")), 1, 3).alias("holder_name_prefix"),
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
    .orderBy(F.col("holder_segment").desc(), "premium")
)
