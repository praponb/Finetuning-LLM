from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((~F.col("claim_type").isin("flood", "medical")) & (F.col("claim_date") < F.lit("2024-12-05").cast("date")))
    .select(
        "status",
        F.col("reported_at").alias("event_time"),
        F.col("id").alias("entity_id"),
    )
)

claim_rows = df_claims.select("claim_type", "reported_at", "adjuster_id")

result = (
    claim_side
    .union(claim_rows)
    .orderBy("status")
)
