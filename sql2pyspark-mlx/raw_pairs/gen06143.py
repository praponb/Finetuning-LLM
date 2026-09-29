from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("temp_c", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("device_code")

result = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.avg("temp_c").over(w).alias("temp_c_avg_5"),
        F.avg("pressure").over(w2).alias("pressure_avg_2"),
        F.corr("pressure", "temp_c").over(w3).alias("corr_pressure_temp_c"),
        F.stddev_samp("pressure").over(w3).alias("device_code_stddev_samp_pressure"),
    )
)
