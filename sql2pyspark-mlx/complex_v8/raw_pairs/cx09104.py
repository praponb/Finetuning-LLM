from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .filter(F.col("level") < 3)
    .select("id", F.col("team").alias("label"))
)

policy_side = (
    df_policies
    .filter((F.col("holder_id") <= 17) & (~F.col("product").isin("HOME", "TRAVEL")))
    .select(F.col("holder_id").alias("id"), F.col("status").alias("label"))
)

result = (
    adjuster_side
    .exceptAll(policy_side)
    .orderBy("id")
)
