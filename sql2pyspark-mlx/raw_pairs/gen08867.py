from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy(F.to_date("reading_ts"), "device_code")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("least_humidity_status_bits", F.least("humidity", "status_bits"))
    .withColumn("temp_c_band", F.when(F.col("temp_c") >= 36.6, "High").when(F.col("temp_c") >= 20.8, "Medium").otherwise("Low"))
)

result = (
    valid
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "firmware",
        "least_humidity_status_bits",
        F.corr("temp_c", "humidity").over(w).alias("corr_temp_c_humidity"),
        F.round((F.col("pressure") / F.sum("pressure").over(w2)), 4).alias("pressure_share"),
    )
)
