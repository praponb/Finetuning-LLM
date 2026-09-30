from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        F.lit("holder").alias("source"),
        "holder_name",
        "joined_on",
        "region",
    )
)

claim_rows = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
    .select(
        F.lit("claim").alias("source"),
        F.col("claim_type").alias("holder_name"),
        F.col("claim_date").alias("joined_on"),
        F.col("status").alias("region"),
    )
)

result = (
    holder_side
    .unionByName(claim_rows)
    .distinct()
)
