from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter((F.col("holder_id").isNotNull()) & (F.col("start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-03-23").cast("date"))))
    .select(F.col("holder_id").alias("entity_id"), "status")
)

holder_rows = (
    df_policyholders
    .select(F.col("id").alias("entity_id"), F.col("holder_name").alias("status"))
)

result = (
    policy_rows
    .unionByName(holder_rows)
    .orderBy("status")
)
