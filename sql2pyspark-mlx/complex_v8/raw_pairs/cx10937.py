from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) & (F.col("kwh") <= 106.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

reading_rows = base_readings.select(F.col("site_id").alias("qty"), "read_at")

tariff_side = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-27 13:00:00").cast("timestamp")) & (F.col("rate") < 6.1))
    .select("id", "valid_to")
)

result = reading_rows.union(tariff_side)
