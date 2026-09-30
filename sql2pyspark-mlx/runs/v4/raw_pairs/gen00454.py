from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("id", "device_code")
    .intersect(
        df_device_readings
        .filter(F.col("status_bits").between(7, 11))
        .select("id", "device_code")
    )
)
