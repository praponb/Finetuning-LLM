from pyspark.sql import functions as F

holder_side = df_policyholders.select(F.col("id").alias("ref_id"))

holder_rows = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-06-18").cast("date")) & (~F.col("region").isin("East", "Central")))
    .select("id")
)

result = (
    holder_side
    .intersectAll(holder_rows)
    .orderBy(F.col("ref_id").desc())
)
