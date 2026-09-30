from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code")
    .exceptAll(
        df_device_readings
        .filter(F.col("temp_c").between(24, 29))
        .select("device_code")
    )
)
