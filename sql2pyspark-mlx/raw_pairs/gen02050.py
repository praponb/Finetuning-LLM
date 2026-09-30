from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(~F.col("device_code").isin(["DV-03", "DV-01"]))
        .select("device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "device_code", "humidity", "pressure", "status_bits", "firmware")
    .orderBy(F.col("pressure").desc_nulls_first(), F.col("device_code").desc())
)
