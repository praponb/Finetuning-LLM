from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 07:00:00").cast("timestamp")) & (F.col("status_bits") < 12))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.initcap(F.trim("firmware")).alias("firmware_title"),
    )
)

result = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "firmware_title",
    )
)
