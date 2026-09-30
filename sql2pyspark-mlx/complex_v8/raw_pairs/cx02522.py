from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "BAD")) & (F.col("site_id") >= 2))
    .select("site_id", "quality")
)

tariff_side = (
    df_tariff_windows
    .filter(F.hour(F.col("valid_to")).between(10, 16))
    .select("id", "tariff")
)

result = (
    reading_side
    .subtract(tariff_side)
    .orderBy("quality", "site_id")
)
