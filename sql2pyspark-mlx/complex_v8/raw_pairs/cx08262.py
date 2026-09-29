from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .select(
        F.lit("reading").alias("record_type"),
        F.col("quality").alias("label"),
        "voltage",
    )
)

reading_side = df_meter_readings.select(F.lit("reading_2").alias("record_type"), "quality", "kwh")

result = reading_rows.union(reading_side)
