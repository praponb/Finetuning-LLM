from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("status_bits") < 9)
        .select("device_code", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "reading_ts", "temp_c", "pressure", "status_bits", "firmware")
    .orderBy(F.col("status_bits").asc_nulls_last())
)
