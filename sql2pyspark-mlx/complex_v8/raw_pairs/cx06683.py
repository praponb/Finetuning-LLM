from pyspark.sql import functions as F

adjuster_side = df_adjusters.select("level")

policy_rows = (
    df_policies
    .filter(F.col("end_date") >= F.lit("2026-02-26").cast("date"))
    .select("id")
)

result = (
    adjuster_side
    .exceptAll(policy_rows)
    .orderBy(F.col("level").desc())
)
