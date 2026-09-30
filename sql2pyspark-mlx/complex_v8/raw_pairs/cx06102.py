from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .select(
        F.col("segment").alias("name"),
        "risk_score",
        F.lit("holder").alias("source"),
    )
)

holder_side_2 = df_policyholders.select("segment", "risk_score", F.lit("holder_2").alias("source"))

result = holder_side.union(holder_side_2)
