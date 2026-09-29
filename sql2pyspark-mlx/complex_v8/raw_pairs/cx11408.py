from pyspark.sql import functions as F

valid = df_policies.filter((F.col("holder_id").isNotNull()) & (F.col("premium") < 3521.8))

policy_side = (
    valid
    .select(
        F.lit("policy").alias("source"),
        F.col("id").alias("key_id"),
        F.col("holder_id").alias("ref_id"),
    )
)

adjuster_side = (
    df_adjusters
    .select(
        F.lit("adjuster").alias("source"),
        F.col("id").alias("key_id"),
        F.col("level").alias("ref_id"),
    )
)

result = (
    policy_side
    .unionByName(adjuster_side)
    .distinct()
    .orderBy("key_id", "source")
)
