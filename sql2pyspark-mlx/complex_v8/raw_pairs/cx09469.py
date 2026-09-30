from pyspark.sql import functions as F

cleaned = (
    df_policyholders
    .filter((F.lower(F.col("holder_name")).like("%le%")) & (F.col("risk_score") >= 0.1))
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        F.date_trunc("year", F.col("joined_on")).alias("joined_on_year"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on")).alias("days_since_joined_on"),
    )
)

scored = (
    cleaned
    .withColumn("days_since_joined_on_year", F.datediff(F.lit("2025-12-31").cast("date"), F.col("joined_on_year")))
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

with_stats = (
    scored.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

result = (
    with_stats
    .groupBy("joined_on_year")
    .pivot("region", ["Central", "North", "East"])
    .agg(F.min("policy_count"))
)
