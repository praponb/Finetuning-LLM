from pyspark.sql import functions as F

result = (
    df_device_readings
    .cube(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("status_bits"))).alias("dv_04_total_status_bits"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
    )
    .orderBy(F.col("dv_04_rows").desc_nulls_first())
)
