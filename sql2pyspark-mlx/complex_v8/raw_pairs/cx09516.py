from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 12)) & (F.col("approved_amount").between(2349.3, 17759.3)))
    .select(
        F.col("claim_type").alias("label"),
        F.col("claim_date").alias("day"),
        "claimed_amount",
    )
)

holder_side = (
    df_policyholders
    .filter((F.col("joined_on").between(F.lit("2024-02-22").cast("date"), F.lit("2025-10-17").cast("date"))) & (F.upper(F.trim(F.col("holder_name"))).like("A%")))
    .select("segment", "joined_on", "risk_score")
)

result = (
    claim_rows
    .union(holder_side)
    .orderBy(F.col("day").desc(), "label")
)
