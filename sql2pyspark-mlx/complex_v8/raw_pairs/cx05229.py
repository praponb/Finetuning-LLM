from pyspark.sql import functions as F

policy_side = (
    df_policies
    .select(F.col("holder_id").alias("key_id"), "id", F.lit("policy").alias("source"))
)

adjuster_rows = (
    df_adjusters
    .filter(F.col("team") == "Health")
    .select(
        F.col("id").alias("key_id"),
        F.col("level").alias("id"),
        F.lit("adjuster").alias("source"),
    )
)

result = (
    policy_side
    .unionByName(adjuster_rows)
    .distinct()
    .orderBy(F.col("id").desc(), "key_id")
)
