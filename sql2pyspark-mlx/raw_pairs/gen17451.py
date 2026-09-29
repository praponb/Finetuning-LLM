from pyspark.sql import functions as F

result = (
    df_device_readings
    .select("device_code", "firmware")
    .exceptAll(
        df_device_readings
        .filter(F.col("humidity") > 70)
        .select("device_code", "firmware")
    )
)
