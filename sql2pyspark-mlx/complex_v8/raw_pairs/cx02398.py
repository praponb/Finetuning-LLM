from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

scored = (
    cleaned_readings
    .filter(F.col("quality") == "OK")
    .select(
        "*",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "kwh_filled",
        F.when(F.col("kwh_filled") >= 78.0, "A").when(F.col("kwh_filled") >= 24.0, "B").otherwise("C").alias("kwh_filled_tier"),
        F.round(F.col("kwh_filled") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("kwh_filled_per_kwh"),
    )
    .orderBy(F.col("site_id").desc())
)
