from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(F.col("id").alias("qty"))
)

tariff_rows = (
    df_tariff_windows
    .filter((F.col("valid_to") < F.lit("2025-03-09 04:00:00").cast("timestamp")) & (F.col("tariff").isin("TOU", "EV")))
    .select(F.col("id").alias("qty"))
)

result = (
    reading_side
    .intersect(tariff_rows)
    .orderBy(F.col("qty").desc())
)
