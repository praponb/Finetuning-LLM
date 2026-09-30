from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter((F.col("premium") <= 3719.7) & (F.col("end_date") < F.lit("2026-07-11").cast("date")))
    .select(F.col("status").alias("category"))
)

adjuster_rows = df_adjusters.select("adjuster_name")

result = (
    policy_rows
    .intersect(adjuster_rows)
    .orderBy(F.col("category").desc())
)
