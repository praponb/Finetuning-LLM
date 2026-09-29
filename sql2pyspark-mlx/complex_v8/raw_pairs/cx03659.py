from pyspark.sql import functions as F

holder_rows = df_policyholders.select(F.col("id").alias("qty"))

adjuster_side = (
    df_adjusters
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("B%"))
    .select(F.col("id").alias("qty"))
)

result = (
    holder_rows
    .subtract(adjuster_side)
    .orderBy("qty")
)
