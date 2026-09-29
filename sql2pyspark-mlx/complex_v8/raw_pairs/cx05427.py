from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 45.0)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

reading_side = base_readings.select(F.col("site_id").alias("entity_id"))

tariff_side = df_tariff_windows.select(F.col("id").alias("entity_id"))

result = (
    reading_side
    .subtract(tariff_side)
    .orderBy(F.col("entity_id").desc())
)
