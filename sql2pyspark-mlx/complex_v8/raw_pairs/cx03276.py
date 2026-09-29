from pyspark.sql import functions as F

valid = (
    df_adjusters
    .filter(F.length(F.trim(F.col("adjuster_name"))) >= 8)
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.abs(F.col("level") - 2).alias("level_distance"),
    )
)

adjuster_side = (
    valid
    .filter((F.col("adjuster_name").isNotNull()) & (F.col("team") == "Health"))
    .select("adjuster_name", F.col("level_distance").alias("entity_id"))
)

policy_side = df_policies.select("status", "id")

result = (
    adjuster_side
    .union(policy_side)
    .orderBy(F.col("entity_id").desc())
)
