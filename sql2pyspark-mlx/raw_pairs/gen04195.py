from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w_hist = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("device_code").orderBy("status_bits")
w4 = Window.orderBy(F.col("pressure").desc(), "id")

result = (
    df_device_readings
    .filter(F.col("firmware") != "1.1.0")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.max("temp_c").over(w).alias("device_code_max_temp_c"),
        F.avg("temp_c").over(w_hist).alias("temp_c_avg_2_prior"),
        F.round((F.col("humidity") / F.sum("humidity").over(w)), 4).alias("humidity_share"),
        F.rank().over(w3).alias("status_bits_rank"),
        F.ntile(5).over(w4).alias("pressure_bucket"),
    )
)
