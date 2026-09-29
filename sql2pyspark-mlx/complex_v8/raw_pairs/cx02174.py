from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter((F.col("segment") == "SME") | (F.col("region").isNotNull()))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.when(F.col("region").isin("West", "Central"), "group_a").when(F.col("region") == "South", "group_b").otherwise("other").alias("region_group"),
    )
)

holder_rows = cleaned_holders.select("id", "region_group")

claim_side = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-01-24").cast("date")) & (F.col("adjuster_id").isNotNull()))
    .select(
        F.col("policy_id").alias("id"),
        F.col("claim_type").alias("region_group"),
    )
)

result = holder_rows.exceptAll(claim_side)
