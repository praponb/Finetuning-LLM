from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2026-11-25").cast("date")))
    .select(
        F.col("id").alias("ref_id"),
        "start_date",
        F.col("premium").alias("value"),
    )
)

holder_rows = (
    df_policyholders
    .filter((F.col("segment").isin("RETAIL", "VIP")) & (F.col("joined_on").between(F.lit("2024-02-22").cast("date"), F.lit("2025-09-02").cast("date"))))
    .select("id", "joined_on", "risk_score")
)

holder_side = df_policyholders.select("id", "joined_on", "risk_score")

result = (
    policy_side
    .union(holder_rows)
    .union(holder_side)
)
