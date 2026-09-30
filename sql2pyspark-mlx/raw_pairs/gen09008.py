from pyspark.sql import functions as F

result = (
    df_device_readings
    .cube(
        F.when(F.col("pressure") >= 1019, "LARGE").otherwise("SMALL").alias("pressure_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("humidity_per_dv_01"),
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("humidity"))).alias("dv_04_avg_humidity"),
        F.percentile_approx("status_bits", [0.25, 0.5, 0.75]).alias("status_bits_quartiles"),
    )
)
