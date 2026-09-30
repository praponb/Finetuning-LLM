from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-18 15:00:00").cast("timestamp")) & (F.col("claim_date") < F.lit("2025-09-19").cast("date")))
    .select(
        F.col("claim_type").alias("label"),
        "reported_at",
        "status",
        F.lit("claim").alias("origin"),
    )
)

claim_rows_2 = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-08-01").cast("date")) & (F.col("reported_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "claim_type",
        "reported_at",
        "status",
        F.lit("claim_2").alias("origin"),
    )
)

unioned = (
    claim_rows
    .union(claim_rows_2)
    .distinct()
)

result = (
    unioned
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("origin")
)
