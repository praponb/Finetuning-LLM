from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .filter((F.hour(F.col("valid_from")).between(7, 9)) & (F.to_date(F.col("valid_to")) < F.lit("2025-03-12").cast("date")))
    .select(
        "id",
        F.col("valid_from").alias("event_time"),
        F.col("tariff").alias("label"),
    )
)

reading_side = df_meter_readings.select("site_id", "read_at", "quality")

tariff_side_2 = df_tariff_windows.select("id", "valid_to", "tariff")

result = (
    tariff_side
    .union(reading_side)
    .union(tariff_side_2)
    .orderBy("label", "event_time")
)
