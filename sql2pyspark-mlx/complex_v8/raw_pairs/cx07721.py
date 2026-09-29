from pyspark.sql import functions as F

reading_side = df_meter_readings.select("site_id")

tariff_rows = (
    df_tariff_windows
    .filter((F.col("rate") > 3.7) & (F.to_date(F.col("valid_from")) < F.lit("2025-03-09").cast("date")))
    .select("id")
)

result = (
    reading_side
    .intersect(tariff_rows)
    .orderBy("site_id")
)
