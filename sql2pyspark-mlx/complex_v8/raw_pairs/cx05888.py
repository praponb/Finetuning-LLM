from pyspark.sql import functions as F

filtered = (
    df_adjusters
    .filter((F.col("level").between(2, 3)) & (F.upper(F.trim(F.col("adjuster_name"))).like("E%")))
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
        F.coalesce(F.col("level"), F.lit(0)).alias("level_filled"),
    )
)

adjuster_rows = filtered.select("id", F.col("level").alias("key_id"))

policy_rows = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select("holder_id", "id")
)

result = (
    adjuster_rows
    .union(policy_rows)
    .distinct()
)
