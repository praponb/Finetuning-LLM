from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2025)
    .select("id", "product")
)

adjuster_side = (
    df_adjusters
    .filter(F.col("level") > 2)
    .select("level", "adjuster_name")
)

result = policy_rows.subtract(adjuster_side)
