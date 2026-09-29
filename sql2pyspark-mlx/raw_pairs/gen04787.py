from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")

result = (
    df_device_readings
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.avg("temp_c").over(w).alias("temp_c_avg_1h"),
        F.ntile(4).over(w2).alias("pressure_bucket"),
    )
)
