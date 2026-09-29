from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity_times_status_bits").asc_nulls_last(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("humidity") * F.col("status_bits")).alias("humidity_times_status_bits"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
