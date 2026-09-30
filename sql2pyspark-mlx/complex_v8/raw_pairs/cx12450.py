from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

reading_side = (
    recent_readings
    .filter((F.col("voltage") > 216.8) & (F.col("quality_slug").isNotNull()))
    .select("site_id", "read_at")
)

tariff_side = df_tariff_windows.select("id", "valid_from")

result = reading_side.union(tariff_side)
