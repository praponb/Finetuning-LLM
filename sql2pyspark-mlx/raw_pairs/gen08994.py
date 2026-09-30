from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.count("*").alias("cnt"),
        F.sum("pressure").alias("total_pressure"),
        F.round(F.sum("temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("temp_c_per_dv_04"),
    )
    .filter(F.col("total_pressure") > 0)
)
