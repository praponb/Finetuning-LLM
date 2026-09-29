from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("pressure") >= 1016, "LARGE").otherwise("SMALL").alias("pressure_tier"),
        "device_code",
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-03", F.col("humidity"))).alias("dv_03_avg_humidity"),
        F.corr("pressure", "humidity").alias("corr_pressure_humidity"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
    )
)
