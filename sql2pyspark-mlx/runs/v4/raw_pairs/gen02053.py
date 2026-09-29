from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last())
w2 = Window.partitionBy("device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code")
w4 = Window.partitionBy("hour_window").orderBy(F.col("num_device_codes").desc())

metrics = (
    df_device_readings
    .select(
        "*",
        F.cume_dist().over(w).alias("humidity_cume_dist"),
        F.sum("status_bits").over(w2).alias("running_status_bits"),
        F.round((F.col("pressure") / F.sum("pressure").over(w3)), 4).alias("pressure_share"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"), "device_code")
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.percentile_approx("running_status_bits", 0.9).alias("running_status_bits_p90"),
    )
    .withColumn("num_device_codes_rank", F.rank().over(w4))
    .orderBy(F.col("num_device_codes").asc_nulls_first())
)
