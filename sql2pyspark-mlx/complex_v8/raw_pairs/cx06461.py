from pyspark.sql import functions as F

recent_holders = (
    df_policyholders
    .filter(F.col("region") != "Central")
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
    )
)

screened = (
    recent_holders.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.start_date") < F.lit("2025-02-13").cast("date")) & (F.col("p.product") == "AUTO"), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "risk_score",
        "segment",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
    .orderBy(F.col("risk_score").asc(), "id")
    .limit(10)
)
