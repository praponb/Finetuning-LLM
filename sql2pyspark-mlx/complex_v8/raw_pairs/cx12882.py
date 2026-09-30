from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2025)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

holder_rows = valid_holders.select(F.col("id").alias("key_id"))

adjuster_rows = df_adjusters.select("level")

result = holder_rows.subtract(adjuster_rows)
