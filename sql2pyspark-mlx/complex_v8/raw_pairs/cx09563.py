from pyspark.sql import functions as F

recent_holders = (
    df_policyholders
    .filter(F.col("risk_score") < 0.8)
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        F.when(F.col("segment").isin("RETAIL", "VIP"), "group_a").when(F.col("segment") == "SME", "group_b").otherwise("other").alias("segment_group"),
    )
)

derived = (
    recent_holders
    .select(
        "id",
        "holder_name",
        "region",
        F.concat_ws(": ", F.col("holder_name"), F.col("region")).alias("holder_name_region_label"),
        F.concat_ws(": ", F.col("segment_group"), F.col("holder_name")).alias("segment_group_holder_name_label"),
        F.coalesce(F.col("segment_group"), F.lit("UNKNOWN")).alias("segment_group_or_unknown"),
    )
)

scored = (
    derived
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
    )
)

line_level = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.region",
        "s.holder_name_region_label",
        "s.segment_group_holder_name_label",
        "s.segment_group_or_unknown",
        "s.region_ref",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    line_level
    .select(
        "id",
        "region",
        "holder_name_region_label",
        "segment_group_or_unknown",
        "region_ref",
        "policy_id",
        "policy_start_date",
    )
    .orderBy(F.col("policy_id").desc())
)
