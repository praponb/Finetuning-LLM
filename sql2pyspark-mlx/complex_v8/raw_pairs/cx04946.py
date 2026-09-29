from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter(F.col("region").isin("East", "West", "North"))
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
    valid_holders
    .filter(F.length(F.trim(F.col("holder_name"))) >= 5)
    .select(F.col("id").alias("qty"))
)

adjuster_rows = df_adjusters.select(F.col("level").alias("qty"))

result = (
    holder_side
    .subtract(adjuster_rows)
    .orderBy(F.col("qty").desc())
)
