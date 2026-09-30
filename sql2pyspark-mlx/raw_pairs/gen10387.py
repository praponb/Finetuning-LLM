from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("firmware")
    .subtract(
        df_device_readings
        .filter(F.col("pressure").between(1005, 1015))
        .select("firmware")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("status_bits") >= 9, "HIGH").otherwise("LOW").alias("status_bits_tier"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("humidity_per_dv_01"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("pressure")).otherwise(0)).alias("dv_04_pressure"),
    )
    .orderBy(F.col("status_bits_bit_or").asc_nulls_first())
)
