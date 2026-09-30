from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_region = Window.partitionBy("region")

recent_holders = (
    df_policyholders
    .filter((F.col("joined_on").between(F.lit("2024-02-22").cast("date"), F.lit("2025-10-17").cast("date"))) & (F.length(F.trim(F.col("holder_name"))) >= 4))
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
    )
)

screened = (
    recent_holders
    .crossJoin(recent_holders.agg(F.avg("risk_score").alias("overall_avg_risk_score")))
    .filter(F.col("risk_score") > F.col("overall_avg_risk_score"))
    .drop("overall_avg_risk_score")
)

derived = (
    screened
    .select(
        "*",
        F.substring(F.trim(F.col("segment_ref")), 1, 2).alias("segment_ref_prefix"),
        F.length(F.trim(F.col("segment_ref"))).alias("segment_ref_len"),
        F.concat(F.col("segment_ref"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref_ref"),
    )
)

metrics = (
    derived
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "segment_ref_prefix",
        "segment_ref_ref",
        F.sum("segment_ref_len").over(w_region).alias("region_sum_segment_ref_len"),
        F.max("segment_ref_len").over(w_region).alias("region_max_segment_ref_len"),
    )
)

result = (
    metrics
    .filter(F.col("region") != "East")
    .select(
        "id",
        "joined_on",
        "segment_ref_ref",
        "region_sum_segment_ref_len",
        "region_max_segment_ref_len",
    )
    .orderBy(F.col("region_max_segment_ref_len").desc(), "id")
    .limit(5)
)
