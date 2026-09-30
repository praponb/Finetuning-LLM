from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-03-27").cast("date"), F.lit("2025-05-28").cast("date")))
    .select("id", "start_date")
)

holder_side = (
    df_policyholders
    .filter((F.col("risk_score") > 0.3) & (F.lower(F.col("holder_name")).like("% n%")))
    .select("id", F.col("joined_on").alias("start_date"))
)

result = (
    policy_rows
    .union(holder_side)
    .distinct()
    .orderBy("start_date")
)
