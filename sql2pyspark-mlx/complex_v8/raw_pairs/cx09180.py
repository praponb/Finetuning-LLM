from pyspark.sql import functions as F

scoped_holders = (
    df_policyholders
    .filter((F.col("segment") != "RETAIL") | (F.col("risk_score") >= 0.3))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
    )
)

holder_rows = scoped_holders.select("id", "joined_on", F.lit("holder").alias("src"))

claim_side = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-03-29").cast("date"))
    .select(
        F.col("policy_id").alias("id"),
        F.col("claim_date").alias("joined_on"),
        F.lit("claim").alias("src"),
    )
)

holder_side = (
    df_policyholders
    .filter(F.col("joined_on") >= F.lit("2024-05-23").cast("date"))
    .select("id", "joined_on", F.lit("holder_3").alias("src"))
)

result = (
    holder_rows
    .union(claim_side)
    .union(holder_side)
    .orderBy(F.col("joined_on").desc(), "src")
)
