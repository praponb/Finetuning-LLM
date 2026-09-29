from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("temp_c", "id").rowsBetween(-6, -1)

result = (
    df_device_readings
    .filter(F.col("pressure") > 1008)
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.avg("temp_c").over(w).alias("device_code_avg_temp_c"),
        F.avg("humidity").over(w2).alias("humidity_avg_6_prior"),
    )
)
