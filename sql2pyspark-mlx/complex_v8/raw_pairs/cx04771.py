from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 2)) & (F.col("quality").isin("EST", "BAD")))
)

reading_side = valid_readings.select(F.col("id").alias("qty"), "read_at")

tariff_rows = (
    df_tariff_windows
    .filter(F.col("tariff") == "TOU")
    .select(F.col("id").alias("qty"), F.col("valid_to").alias("read_at"))
)

result = (
    reading_side
    .unionByName(tariff_rows)
    .distinct()
    .orderBy("read_at", "qty")
)
