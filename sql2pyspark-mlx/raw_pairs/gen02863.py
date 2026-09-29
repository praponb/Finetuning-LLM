from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())
w_ordered = Window.partitionBy("device_code")
w3 = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy(F.col("first_known_pressure").asc_nulls_last(), "id")

deduped = (
    df_device_readings
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "device_code", "temp_c", "humidity", "pressure")
)

enriched = (
    deduped
    .filter(F.col("temp_c") >= 23)
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        F.corr("temp_c", "pressure").over(w_ordered).alias("corr_temp_c_pressure"),
        F.avg("pressure").over(w_ordered).alias("device_code_avg_pressure"),
        F.first("pressure", ignorenulls=True).over(w3).alias("first_known_pressure"),
    )
)

with_windows = (
    enriched
    .withColumn("running_count_temp_c", F.count("temp_c").over(w4))
    .withColumn("running_sum_temp_c", F.sum("temp_c").over(w4))
    .withColumn("running_max_temp_c", F.max("temp_c").over(w4))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "pressure")
)

result = (
    latest
    .groupBy(
        F.when(F.col("pressure") >= 1032, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
    )
    .agg(
        F.max("pressure").alias("max_pressure"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.percentile_approx("pressure", 0.9).alias("pressure_p90"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.percentile_approx("pressure", 0.95).alias("pressure_p95"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
    )
    .orderBy(F.col("pressure_p95").desc_nulls_last())
)
