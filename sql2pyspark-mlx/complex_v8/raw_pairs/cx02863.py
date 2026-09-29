from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "LIFE"))
    .select(F.col("holder_id").alias("ref_id"))
)

adjuster_side = df_adjusters.select("id")

result = policy_rows.subtract(adjuster_side)
