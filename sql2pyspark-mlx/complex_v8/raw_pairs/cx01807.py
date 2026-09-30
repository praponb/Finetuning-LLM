from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("voltage").between(216.8, 240.1)) & (F.col("kwh") <= 106.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

scored = (
    filtered_readings
    .filter(F.col("quality_or_unknown").isin("EST", "UNKNOWN", "OK"))
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "quality_or_unknown",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.concat_ws("|", F.col("quality"), F.col("quality_or_unknown")).alias("quality_quality_or_unknown_label"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
)

tagged = (
    scored
    .select(
        "id",
        "kwh",
        "quality_or_unknown",
        "quality_or_unknown_or_unknown",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

labeled = (
    tagged
    .select(
        "id",
        "quality_clean",
        F.when(F.col("quality_or_unknown").isin("OK", "UNKNOWN"), "group_a").when(F.col("quality_or_unknown") == "EST", "group_b").otherwise("other").alias("quality_or_unknown_group"),
        F.lower(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
    )
)

summary = (
    labeled
    .groupBy("quality_or_unknown_group")
    .agg(F.countDistinct("id").alias("distinct_ids"))
)

screened = (
    summary
    .crossJoin(summary.agg(F.max("distinct_ids").alias("overall_max_distinct_ids")))
    .filter(F.col("distinct_ids") < F.col("overall_max_distinct_ids"))
    .drop("overall_max_distinct_ids")
)

result = (
    screened
    .select("quality_or_unknown_group", "distinct_ids")
    .orderBy(F.col("distinct_ids").desc(), F.col("quality_or_unknown_group").desc())
)
