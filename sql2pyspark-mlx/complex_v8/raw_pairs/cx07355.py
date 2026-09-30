from pyspark.sql import functions as F

recent = (
    df_policyholders
    .filter(F.col("segment") != "VIP")
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

segment_stats = (
    recent
    .groupBy("segment")
    .agg(F.avg("risk_score").alias("avg_risk_score"))
)

compared = (
    recent.alias("s")
    .join(segment_stats.alias("g"), F.col("g.segment") == F.col("s.segment"), "inner")
    .filter(F.col("s.risk_score") > F.col("g.avg_risk_score"))
    .select(
        "s.*",
        "g.avg_risk_score",
        F.round(F.col("s.risk_score") - F.col("g.avg_risk_score"), 2).alias("risk_score_minus_avg"),
    )
)

with_activity = (
    compared.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.end_date") < F.lit("2026-08-31").cast("date")), "left_anti")
)

result = (
    with_activity
    .filter(F.col("risk_score") >= 0.3)
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "avg_risk_score",
        "risk_score_minus_avg",
    )
    .orderBy(F.col("region").desc(), "id")
    .limit(10)
)
