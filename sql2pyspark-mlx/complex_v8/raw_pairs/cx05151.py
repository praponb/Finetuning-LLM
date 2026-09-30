from pyspark.sql import functions as F

prepared_holders = (
    df_policyholders
    .filter((F.col("region").isNotNull()) & (F.col("risk_score") <= 0.6))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

holder_side = (
    prepared_holders
    .filter((F.col("risk_score") > 0.1) & (F.col("region") == "East"))
    .select("id")
)

adjuster_rows = (
    df_adjusters
    .filter((~F.col("team").isin("Property", "Travel")) & (F.col("level") < 3))
    .select(F.col("level").alias("id"))
)

result = (
    holder_side
    .intersect(adjuster_rows)
    .orderBy("id")
)
