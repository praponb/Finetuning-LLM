from pyspark.sql import functions as F

claim_side = (
    df_claims
    .select(
        "id",
        F.col("reported_at").alias("happened_at"),
        F.col("claim_date").alias("event_date"),
        F.lit("claim").alias("record_type"),
    )
)

claim_side_2 = (
    df_claims
    .filter((F.col("claim_type") != "flood") & (F.col("claim_date") < F.lit("2025-08-25").cast("date")))
    .select(
        "id",
        "reported_at",
        "claim_date",
        F.lit("claim_2").alias("record_type"),
    )
)

claim_side_3 = (
    df_claims
    .filter(F.col("claim_type") == "collision")
    .select(
        "policy_id",
        "reported_at",
        "claim_date",
        F.lit("claim_3").alias("record_type"),
    )
)

result = (
    claim_side
    .union(claim_side_2)
    .union(claim_side_3)
    .orderBy("happened_at", "event_date")
)
