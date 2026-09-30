from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-10-23").cast("date")) & (~F.col("segment").isin("VIP", "SME")))
    .select(
        F.col("id").alias("qty"),
        F.col("region").alias("category"),
        F.lit("holder").alias("origin"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("id").alias("qty"),
        F.col("status").alias("category"),
        F.lit("claim").alias("origin"),
    )
)

result = (
    holder_rows
    .unionByName(claim_rows)
    .distinct()
)
