from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("pressure") < 1005)
    .select("*", F.round("status_bits", 0).alias("status_bits_rounded"))
)

result = (
    valid
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("firmware") == "2.0.1")
        .select("firmware", "device_code")
    )
)
