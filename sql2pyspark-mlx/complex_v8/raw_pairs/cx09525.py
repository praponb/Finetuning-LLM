from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .select(
        F.col("id").alias("key_id"),
        "risk_score",
        "joined_on",
        F.lit("holder").alias("src"),
    )
)

holder_side = (
    df_policyholders
    .filter((F.col("region").isin("East", "West", "Central")) & (F.col("risk_score") < 0.8))
    .select(
        "id",
        "risk_score",
        "joined_on",
        F.lit("holder_2").alias("src"),
    )
)

result = holder_rows.union(holder_side)
