from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2024-04-08")
        .select("device_code")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
    )
    .orderBy("id")
)
