from pyspark.sql import functions as F

reading_side = (
    df_meter_readings
    .select(F.col("id").alias("entity_id"), F.col("quality").alias("name"))
)

tariff_side = (
    df_tariff_windows
    .filter(F.col("rate") > 4.2)
    .select("id", "tariff")
)

result = (
    reading_side
    .intersect(tariff_side)
    .orderBy(F.col("entity_id").desc())
)
