from pyspark.sql import functions as F

valid = df_meter_readings.filter(F.hour(F.col("read_at")).between(7, 9))

reading_side = (
    valid
    .filter(F.month(F.col("read_at")).isin(1, 3))
    .select("id", "kwh", "site_id")
)

reading_side_2 = (
    df_meter_readings
    .select(
        F.col("site_id").alias("id"),
        F.col("voltage").alias("kwh"),
        F.col("id").alias("site_id"),
    )
)

result = (
    reading_side
    .union(reading_side_2)
    .distinct()
)
