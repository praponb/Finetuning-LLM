from pyspark.sql import functions as F

matched_keys = (
    df_device_readings
    .select("device_code", "firmware")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") >= "2023-04-01")
        .select("device_code", "firmware")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("humidity") >= 82, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.percentile_approx("status_bits", 0.5).alias("status_bits_p50"),
    )
    .orderBy(F.col("distinct_device_codes").desc_nulls_first())
)
