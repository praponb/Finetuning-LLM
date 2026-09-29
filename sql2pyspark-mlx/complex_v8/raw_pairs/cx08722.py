from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter((~F.col("segment").isin("SME", "RETAIL")) & (F.col("joined_on") >= F.lit("2024-09-04").cast("date")))
    .select("id", F.col("region").alias("name"))
)

policy_side = (
    df_policies
    .filter((F.col("product").isin("LIFE", "HEALTH", "TRAVEL")) & (F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-05-30").cast("date"))))
    .select("id", "status")
)

result = (
    holder_rows
    .intersect(policy_side)
    .orderBy("id")
)
