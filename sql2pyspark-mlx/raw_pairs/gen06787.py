from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("temp_c") <= 36)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.round((F.col("pressure") * 1.15), 2).alias("pressure_scaled"),
        (F.col("status_bits") * F.col("humidity")).alias("status_bits_times_humidity"),
    )
)

result = (
    valid
    .select("id", "device_code", "humidity", F.col("pressure").alias("PRESSURE"))
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "PRESSURE"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
