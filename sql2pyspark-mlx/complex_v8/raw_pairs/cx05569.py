from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter(F.col("joined_on").isNotNull())
    .select(F.col("id").alias("qty"))
)

adjuster_side = df_adjusters.select(F.col("level").alias("qty"))

result = (
    holder_rows
    .intersect(adjuster_side)
    .orderBy("qty")
)
