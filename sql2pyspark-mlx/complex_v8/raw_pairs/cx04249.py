from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .filter(F.col("region").isin("Central", "North", "South"))
    .select(F.col("id").alias("ref_id"))
)

adjuster_rows = (
    df_adjusters
    .filter((F.col("level") < 3) & (F.col("team") != "Travel"))
    .select(F.col("level").alias("ref_id"))
)

result = (
    holder_rows
    .exceptAll(adjuster_rows)
    .orderBy("ref_id")
)
