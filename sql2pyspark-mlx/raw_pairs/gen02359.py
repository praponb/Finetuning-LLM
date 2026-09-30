from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2024-05-27")
    .withColumn("days_since_reading_ts", F.datediff(F.current_date(), "reading_ts"))
    .withColumn("status_bits_rounded", F.round("status_bits", 2))
)

result = (
    filtered
    .select("firmware", "device_code")
    .exceptAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(6, 14))
        .select("firmware", "device_code")
    )
)
