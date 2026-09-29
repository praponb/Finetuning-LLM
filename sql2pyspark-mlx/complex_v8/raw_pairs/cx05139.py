from pyspark.sql import functions as F

adjuster_side = (
    df_adjusters
    .filter(~F.col("team").isin("Health", "Travel"))
    .select("team", "level")
)

holder_rows = (
    df_policyholders
    .filter((F.col("region") == "Central") & (F.col("joined_on").between(F.lit("2024-05-23").cast("date"), F.lit("2025-05-01").cast("date"))))
    .select("holder_name", "id")
)

all_events = adjuster_side.union(holder_rows)

result = (
    all_events
    .groupBy("team")
    .agg(F.count("*").alias("n_rows"), F.sum("level").alias("total_level"))
    .orderBy("team")
)
