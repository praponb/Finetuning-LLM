from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")

result = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-01", "DV-02"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        F.ntile(10).over(w).alias("humidity_bucket"),
        F.ntile(4).over(w2).alias("temp_c_bucket"),
    )
)
