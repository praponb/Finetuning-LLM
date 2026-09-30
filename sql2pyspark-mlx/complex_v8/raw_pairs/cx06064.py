from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter((F.year(F.col("joined_on")) == 2024) & (F.col("segment") != "VIP"))
    .select(F.lit("holder").alias("source"), "holder_name", "segment")
)

claim_rows = df_claims.select(F.lit("claim").alias("source"), "status", "claim_type")

all_events = holder_side.union(claim_rows)

result = (
    all_events
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("source")
)
