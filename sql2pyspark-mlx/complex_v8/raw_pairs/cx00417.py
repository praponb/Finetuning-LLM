from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 12)) & (F.col("site_id") <= 3))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

reading_rows = recent.select(F.col("id").alias("entity_id"), "quality_clean")

tariff_rows = (
    df_tariff_windows
    .filter(F.col("valid_from") < F.lit("2025-02-18 04:00:00").cast("timestamp"))
    .select(F.col("id").alias("entity_id"), F.col("tariff").alias("quality_clean"))
)

result = reading_rows.intersect(tariff_rows)
