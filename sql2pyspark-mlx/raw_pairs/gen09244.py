from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("firmware") != "1.1.0")
        .select("id", "device_code")
    )
)
