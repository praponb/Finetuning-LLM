from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter(F.col("kwh").between(20.3, 81.3))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 68.1).alias("kwh_distance"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage_2"),
    )
)

qualified = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("m.voltage") <= 229.1), "left_semi")
)

labeled = (
    qualified
    .filter(F.col("kwh_distance") <= 78.0)
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "kwh_distance",
        "kwh_per_voltage",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

qualified_2 = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

result = (
    qualified_2
    .select(
        "id",
        "site_id",
        "kwh_distance",
        "kwh_per_voltage",
        "quality_or_unknown",
        "quality_slug",
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
    .orderBy(F.col("kwh_per_voltage").desc(), F.col("site_id").desc())
)
