from pyspark.sql import functions as F

holder_rows = df_policyholders.select(F.col("holder_name").alias("label"), "segment", "joined_on")

holder_side = (
    df_policyholders
    .filter(F.col("region") != "West")
    .select(
        F.col("region").alias("label"),
        F.col("holder_name").alias("segment"),
        "joined_on",
    )
)

claim_rows = (
    df_claims
    .filter(F.col("adjuster_id") >= 4)
    .select(
        F.col("status").alias("label"),
        F.col("claim_type").alias("segment"),
        F.col("claim_date").alias("joined_on"),
    )
)

result = (
    holder_rows
    .unionByName(holder_side)
    .unionByName(claim_rows)
    .orderBy("segment")
)
