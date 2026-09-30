from pyspark.sql import functions as F

policy_rows = df_policies.select(F.lit("policy").alias("source"), "holder_id", "product")

adjuster_side = (
    df_adjusters
    .select(
        F.lit("adjuster").alias("source"),
        F.col("id").alias("holder_id"),
        F.col("team").alias("product"),
    )
)

result = policy_rows.unionByName(adjuster_side)
