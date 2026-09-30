from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("device_code", "id")
    .subtract(
        df_device_readings
        .filter(F.col("pressure").between(992, 1010))
        .select("device_code", "id")
    )
)

reconciled = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "reading_ts", "pressure")
    .orderBy(F.col("reading_ts").desc_nulls_last())
)
