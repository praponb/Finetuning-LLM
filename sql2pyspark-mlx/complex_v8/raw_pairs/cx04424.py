from pyspark.sql import functions as F

filtered = (
    df_policyholders
    .filter((F.col("segment") == "VIP") & (F.col("region").isin("West", "Central")))
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
    )
)

holder_rows = (
    df_policyholders
    .filter((F.length(F.trim(F.col("holder_name"))) >= 8) & (~F.col("segment").isin("RETAIL", "VIP")))
    .select(
        "id",
        F.col("region").alias("category"),
        "risk_score",
        F.lit("holder").alias("origin"),
    )
)

claim_side = (
    df_claims
    .select(
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        F.lit("claim").alias("origin"),
    )
)

stacked = (
    holder_rows
    .union(claim_side)
    .distinct()
)

result = (
    stacked
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("origin")
)
