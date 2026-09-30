from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("humidity") >= 83, "LARGE").otherwise("SMALL").alias("humidity_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.round(F.sum("status_bits") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("status_bits_per_dv_04"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
)
