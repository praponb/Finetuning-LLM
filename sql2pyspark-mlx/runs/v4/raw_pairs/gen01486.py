from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("temp_c") <= 33)
    .select(
        "*",
        (F.col("status_bits") - F.col("temp_c")).alias("status_bits_minus_temp_c"),
    )
)

result = (
    valid
    .select(
        "id",
        "device_code",
        "pressure",
        "humidity",
        "temp_c",
        "status_bits_minus_temp_c",
    )
    .unpivot(
        ["id", "device_code"],
        ["pressure", "humidity", "temp_c", "status_bits_minus_temp_c"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
