from pyspark.sql import functions as F

filtered = (
    df_policyholders
    .filter((F.col("joined_on") < F.lit("2025-07-02").cast("date")) & (F.col("risk_score") <= 0.6))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

screened = (
    filtered
    .crossJoin(filtered.agg(F.avg("risk_score_distance").alias("overall_avg_risk_score_distance")))
    .filter(F.col("risk_score_distance") > F.col("overall_avg_risk_score_distance"))
    .drop("overall_avg_risk_score_distance")
)

derived = (
    screened
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "segment_ref",
        "risk_score_distance",
        F.lower(F.regexp_replace(F.trim(F.col("holder_name")), " ", "_")).alias("holder_name_slug"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
        F.abs(F.col("risk_score_distance") - 0.3).alias("risk_score_distance_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "risk_score_distance",
        "holder_name_slug",
        "segment_or_unknown",
        F.concat_ws(": ", F.col("segment"), F.col("segment_or_unknown")).alias("segment_segment_or_unknown_label"),
        F.dayofweek(F.col("joined_on")).alias("joined_on_dayofweek"),
    )
)
