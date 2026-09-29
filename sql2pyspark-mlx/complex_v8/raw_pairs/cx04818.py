from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

scored = (
    base_readings
    .filter(F.col("voltage") <= 238.3)
    .select(
        "*",
        F.when(F.col("voltage") >= 233.1, "HIGH").when(F.col("voltage") >= 220.7, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown_2"),
    )
)

labeled = (
    scored
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_tier",
        "quality_or_unknown",
        F.substring(F.trim(F.col("voltage_tier")), 1, 4).alias("voltage_tier_prefix"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at_2"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "voltage_tier",
        "quality_or_unknown",
        "voltage_tier_prefix",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.round(F.col("voltage") * 1.1, 2).alias("voltage_adj"),
    )
    .orderBy(F.col("site_id").desc(), F.col("id").desc())
)
