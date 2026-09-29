from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter((F.col("risk_score") <= 0.8) & (F.col("joined_on").between(F.lit("2024-03-22").cast("date"), F.lit("2025-10-17").cast("date"))))
    .select("id")
)

policy_rows = (
    df_policies
    .filter((F.col("premium") < 2844.1) & (F.col("product").isNotNull()))
    .select(F.col("holder_id").alias("id"))
)

result = holder_rows.intersectAll(policy_rows)
