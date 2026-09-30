from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("temp_c") >= 23)
        .select("firmware", "device_code")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1032, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .cube("pressure_tier", "device_code", "activity_day")
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.grouping("pressure_tier").alias("grouping_level"),
    )
)
