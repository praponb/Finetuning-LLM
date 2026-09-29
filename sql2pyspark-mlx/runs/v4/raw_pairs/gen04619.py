from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        F.length("device_code").alias("device_code_length"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    ranked
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 13:00:00").cast("timestamp"))
    .withColumn("pressure_plus_temp_c", (F.col("pressure") + F.col("temp_c")))
)

result = (
    filtered
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "device_code_length",
        "pressure_plus_temp_c",
    )
)
