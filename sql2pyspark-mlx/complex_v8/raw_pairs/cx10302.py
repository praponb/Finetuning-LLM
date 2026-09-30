from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("kwh") <= 81.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

result = (
    base
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.abs(F.col("voltage") - 233.2).alias("voltage_distance"),
    )
    .orderBy(
        F.col("voltage_distance").asc_nulls_last(),
        F.col("voltage").asc_nulls_last(),
        "id",
    )
    .limit(5)
)
