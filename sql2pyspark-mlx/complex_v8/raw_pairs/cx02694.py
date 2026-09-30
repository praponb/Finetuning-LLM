from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(F.col("holder_id") <= 5)
    .select("holder_id", F.col("status").alias("label"))
)

adjuster_side = (
    df_adjusters
    .filter((F.col("team") != "Property") & (F.col("level") < 3))
    .select(
        F.col("level").alias("holder_id"),
        F.col("adjuster_name").alias("label"),
    )
)

result = (
    policy_side
    .intersect(adjuster_side)
    .orderBy(F.col("holder_id").desc(), "label")
)
