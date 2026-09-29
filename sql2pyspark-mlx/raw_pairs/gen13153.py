from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2023-08-03")
        .select("firmware", "device_code")
    )
)
