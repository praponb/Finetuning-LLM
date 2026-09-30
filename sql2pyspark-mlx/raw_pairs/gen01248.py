from pyspark.sql import functions as F

result = (
    df_device_readings
    .withColumn("hour_window", F.date_trunc("HOUR", "reading_ts"))
    .cube("device_code", "hour_window")
    .agg(
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.corr("pressure", "status_bits").alias("corr_pressure_status_bits"),
        F.grouping("device_code").alias("grouping_id"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
