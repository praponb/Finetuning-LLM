from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2025)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region_or_unknown"),
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

grouped = (
    cleaned_holders
    .filter(F.col("joined_on") < F.lit("2025-07-03").cast("date"))
    .groupBy("region_or_unknown")
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("n_region_central"),
        F.round(F.sum("risk_score"), 2).alias("total_risk_score"),
    )
)

result = (
    grouped
    .groupBy("n_region_central")
    .pivot("region_or_unknown", ["West", "UNKNOWN", "South"])
    .agg(F.sum("total_risk_score").alias("amount"), F.count("total_risk_score").alias("n"))
)
