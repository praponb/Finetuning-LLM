from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .filter((F.col("team") == "Travel") & (F.length(F.trim(F.col("adjuster_name"))) >= 6))
    .select(F.col("level").alias("key_id"))
)

policy_side = (
    df_policies
    .filter((F.col("status") == "LAPSED") & (F.col("start_date").between(F.lit("2024-07-19").cast("date"), F.lit("2025-08-03").cast("date"))))
    .select("holder_id")
)

result = (
    adjuster_side
    .subtract(policy_side)
    .orderBy("key_id")
)
