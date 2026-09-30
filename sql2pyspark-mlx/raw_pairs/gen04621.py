from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("least_temp_c_pressure").desc_nulls_last())

filtered = (
    df_device_readings
    .filter(F.col("humidity") >= 46)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") + F.col("temp_c")).alias("status_bits_plus_temp_c"),
        F.least("temp_c", "pressure").alias("least_temp_c_pressure"),
    )
)

latest = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    latest
    .select("device_code")
    .subtract(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-04", "DV-02", "DV-03"]))
        .select("device_code")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select("*", F.cume_dist().over(w2).alias("least_temp_c_pressure_cume_dist"))
)

result = (
    enriched
    .select(
        "device_code",
        "pressure",
        "status_bits",
        "least_temp_c_pressure",
        "latest_rank",
    )
)
