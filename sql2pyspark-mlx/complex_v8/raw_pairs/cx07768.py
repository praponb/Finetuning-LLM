from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter(F.col("risk_score") > 0.2)
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
    )
)

enriched = (
    valid_holders
    .select(
        "*",
        F.date_add(F.col("joined_on"), 30).alias("joined_on_plus_30d"),
        F.concat_ws(": ", F.col("holder_name"), F.col("segment")).alias("holder_name_segment_label"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)

segment_stats = (
    enriched
    .groupBy("segment")
    .agg(F.avg("risk_score").alias("avg_risk_score"))
)

vs_group = (
    enriched.alias("s")
    .join(segment_stats.alias("g"), F.col("g.segment") == F.col("s.segment"), "inner")
    .filter(F.col("s.risk_score") > F.col("g.avg_risk_score"))
    .select(
        "s.*",
        "g.avg_risk_score",
        F.round(F.col("s.risk_score") - F.col("g.avg_risk_score"), 2).alias("risk_score_minus_avg"),
    )
)

labeled = vs_group.withColumn("joined_on_label", F.date_format(F.col("joined_on"), "yyyy-MM"))

result = (
    labeled
    .groupBy("joined_on_plus_30d")
    .pivot("segment", ["VIP", "RETAIL"])
    .agg(F.sum("risk_score").alias("total"), F.count("risk_score").alias("n"))
    .orderBy("joined_on_plus_30d")
)
