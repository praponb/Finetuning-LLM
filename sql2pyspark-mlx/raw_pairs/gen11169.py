from pyspark.sql import functions as F

remaining_keys = (
    df_device_readings
    .select("device_code", "id")
    .intersect(
        df_device_readings
        .filter(F.col("pressure") < 1028)
        .select("device_code", "id")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("pressure_tier", F.when(F.col("pressure") >= 1028, "HIGH").otherwise("LOW"))
    .withColumn("hour_window", F.date_trunc("hour", "reading_ts"))
    .rollup("pressure_tier", "hour_window")
    .agg(
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min_2"),
        F.grouping("pressure_tier").alias("grouping_level"),
    )
)
