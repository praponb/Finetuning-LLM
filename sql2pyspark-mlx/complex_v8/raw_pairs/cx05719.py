from pyspark.sql import functions as F

filtered = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-07-18").cast("date")) & (F.lower(F.col("holder_name")).like("%o %")))
)

holder_side = filtered.select("segment")

adjuster_rows = (
    df_adjusters
    .filter((F.lower(F.col("adjuster_name")).like("%on%")) & (F.col("level") < 3))
    .select(F.col("adjuster_name").alias("segment"))
)

result = (
    holder_side
    .intersect(adjuster_rows)
    .orderBy(F.col("segment").desc())
)
