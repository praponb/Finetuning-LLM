from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("pressure") >= 1003)
    .select("*", F.substring("firmware", 1, 4).alias("firmware_prefix"))
)

result = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "firmware_prefix",
    )
)
