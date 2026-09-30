from pyspark.sql import functions as F

valid = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
    )
)

policy_side = (
    valid
    .filter((F.col("product") == "HEALTH") & (F.col("end_date").between(F.lit("2025-04-17").cast("date"), F.lit("2026-09-19").cast("date"))))
    .select(F.col("holder_id").alias("ref_id"), "product")
)

holder_side = (
    df_policyholders
    .filter(F.col("risk_score") > 0.2)
    .select("id", "segment")
)

result = policy_side.exceptAll(holder_side)
