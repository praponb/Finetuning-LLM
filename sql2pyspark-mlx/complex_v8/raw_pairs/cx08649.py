from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter(F.col("risk_score") > 0.2)
    .select(
        "segment",
        F.col("region").alias("label"),
        F.lit("holder").alias("origin"),
    )
)

holder_side = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-03-22").cast("date")) & (F.col("region") != "East"))
    .select(
        "segment",
        F.col("region").alias("label"),
        F.lit("holder_2").alias("origin"),
    )
)

claim_side = (
    df_claims
    .filter((F.col("claim_type").isin("fire", "collision")) & (F.col("claim_date").between(F.lit("2024-06-20").cast("date"), F.lit("2025-11-29").cast("date"))))
    .select(
        F.col("claim_type").alias("segment"),
        F.col("status").alias("label"),
        F.lit("claim").alias("origin"),
    )
)

all_events = (
    holder_rows
    .unionByName(holder_side)
    .unionByName(claim_side)
)

result = (
    all_events
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("origin")
)
