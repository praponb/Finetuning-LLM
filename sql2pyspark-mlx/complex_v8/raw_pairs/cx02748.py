from pyspark.sql import functions as F

prepared = (
    df_adjusters
    .filter((F.length(F.trim(F.col("adjuster_name"))) >= 7) | (F.col("team").isin("Property", "Travel", "Health")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

adjuster_side = (
    prepared
    .filter(F.col("team") == "Travel")
    .select(F.col("id").alias("ref_id"), F.col("adjuster_name").alias("name"))
)

claim_side = df_claims.select("policy_id", "claim_type")

result = adjuster_side.intersectAll(claim_side)
