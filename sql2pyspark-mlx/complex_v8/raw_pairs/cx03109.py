from pyspark.sql import functions as F

policy_rows = df_policies.select(F.col("id").alias("entity_id"))

adjuster_rows = (
    df_adjusters
    .filter(F.col("team") == "Travel")
    .select(F.col("id").alias("entity_id"))
)

result = policy_rows.intersect(adjuster_rows)
