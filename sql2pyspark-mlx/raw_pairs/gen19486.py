from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity_plus_temp_c").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("humidity", "id")
w5 = Window.orderBy("pressure", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first(), "id")

valid = (
    df_device_readings
    .filter(F.col("pressure").between(995, 996))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 64, "High").when(F.col("humidity") >= 48, "Medium").otherwise("Low"))
    .withColumn("humidity_plus_temp_c", (F.col("humidity") + F.col("temp_c")))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        "humidity_tier",
        "humidity_plus_temp_c",
        "is_new_burst",
    )
)

with_windows = (
    latest
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "firmware",
        "humidity_tier",
        "is_new_burst",
        F.lag("humidity_plus_temp_c", 2).over(w4).alias("prev_humidity_plus_temp_c"),
        F.lead("is_new_burst", 1).over(w5).alias("next_is_new_burst"),
    )
)

latest_device_readings = (
    with_windows
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_device_readings
    .groupBy(
        "device_code",
        F.when(F.col("prev_humidity_plus_temp_c") >= 69, "HIGH").otherwise("LOW").alias("prev_humidity_plus_temp_c_tier"),
    )
    .agg(
        F.stddev_samp("humidity").alias("humidity_stddev"),
        F.percentile_approx("next_is_new_burst", [0.25, 0.5, 0.75]).alias("next_is_new_burst_quartiles"),
        F.avg("prev_humidity_plus_temp_c").alias("avg_prev_humidity_plus_temp_c"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.round(F.sum("humidity") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("humidity_per_dv_04"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
    )
    .filter(F.col("avg_prev_humidity_plus_temp_c") > 0)
    .orderBy(F.col("humidity_stddev").asc_nulls_last())
)
