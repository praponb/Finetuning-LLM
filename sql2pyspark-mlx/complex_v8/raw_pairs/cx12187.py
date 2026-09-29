from pyspark.sql import functions as F

policy_rows = df_policies.select("id")

adjuster_rows = (
    df_adjusters
    .filter((F.col("team") != "Property") & (F.upper(F.trim(F.col("adjuster_name"))).like("F%")))
    .select(F.col("level").alias("id"))
)

result = (
    policy_rows
    .intersect(adjuster_rows)
    .orderBy(F.col("id").desc())
)
