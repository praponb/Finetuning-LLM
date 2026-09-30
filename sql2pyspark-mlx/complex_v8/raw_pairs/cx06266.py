from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter((F.col("risk_score") < 0.6) & (F.col("region") == "West"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.upper(F.trim(F.col("segment"))).alias("segment_clean"),
    )
)

summary = (
    valid_holders
    .groupBy("segment", "region")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("risk_score").alias("total_risk_score"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
    )
    .filter(F.col("row_count") >= 3)
)

with_activity = (
    summary
    .crossJoin(summary.agg(F.avg("avg_risk_score").alias("overall_avg_avg_risk_score")))
    .filter(F.col("avg_risk_score") > F.col("overall_avg_avg_risk_score"))
    .drop("overall_avg_avg_risk_score")
)

result = (
    with_activity
    .groupBy("segment")
    .pivot("region", ["Central", "East"])
    .agg(F.sum("total_risk_score"))
    .orderBy("segment")
)
