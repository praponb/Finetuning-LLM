from pyspark.sql import functions as F

holder_side = df_policyholders.select("id", "joined_on")

claim_rows = (
    df_claims
    .select(
        F.col("adjuster_id").alias("id"),
        F.col("claim_date").alias("joined_on"),
    )
)

result = (
    holder_side
    .unionByName(claim_rows)
    .orderBy(F.col("joined_on").desc(), "id")
)
