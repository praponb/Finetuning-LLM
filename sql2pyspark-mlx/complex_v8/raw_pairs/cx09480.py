from pyspark.sql import functions as F

tariff_rows = (
    df_tariff_windows
    .filter(F.col("tariff") == "TOU")
    .select(
        F.col("tariff").alias("name"),
        F.col("id").alias("entity_id"),
        "rate",
        F.lit("tariff").alias("src"),
    )
)

reading_rows = (
    df_meter_readings
    .select(
        "quality",
        "site_id",
        "voltage",
        F.lit("reading").alias("src"),
    )
)

result = tariff_rows.union(reading_rows)
