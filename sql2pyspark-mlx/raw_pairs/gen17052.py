from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-04", "DV-02", "DV-03"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.concat("firmware", F.lit(", "), "device_code").alias("firmware_device_code"),
        (F.col("temp_c") * F.col("pressure")).alias("temp_c_times_pressure"),
    )
)

result = (
    cleaned
    .select("firmware", "device_code")
    .intersect(
        df_device_readings
        .filter(F.col("firmware").isin(["1.0.3", "2.0.1", "1.1.0"]))
        .select("firmware", "device_code")
    )
)
