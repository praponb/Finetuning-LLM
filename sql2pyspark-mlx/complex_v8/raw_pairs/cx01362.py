from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-02-14").cast("date")) & (F.col("product") == "HEALTH"))
    .select(
        F.lit("policy").alias("src"),
        F.col("product").alias("name"),
        F.col("end_date").alias("day"),
        "id",
    )
)

claim_side = (
    df_claims
    .select(
        F.lit("claim").alias("src"),
        F.col("status").alias("name"),
        F.col("claim_date").alias("day"),
        "id",
    )
)

holder_side = (
    df_policyholders
    .filter((F.col("segment") == "VIP") & (F.length(F.trim(F.col("holder_name"))) >= 7))
    .select(
        F.lit("holder").alias("src"),
        F.col("holder_name").alias("name"),
        F.col("joined_on").alias("day"),
        "id",
    )
)

result = (
    policy_side
    .unionByName(claim_side)
    .unionByName(holder_side)
    .orderBy(F.col("src").desc())
)
