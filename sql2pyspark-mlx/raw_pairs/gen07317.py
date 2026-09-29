from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy(F.year("reading_ts"), "device_code")
w3 = Window.partitionBy("device_code")
w_grp = Window.partitionBy("device_code").orderBy("status_bits_rounded", "id")
w_part = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy("temp_c", "id")
w6 = Window.partitionBy("device_code").orderBy("humidity_share", "id")
w7 = Window.orderBy("humidity_share", "id")
w8 = Window.partitionBy(F.month("reading_ts"), "device_code").orderBy(F.col("humidity").desc())
w9 = Window.partitionBy(F.year("reading_ts"), "device_code").orderBy(F.col("prev_status_bits").desc(), "id")
w10 = Window.partitionBy("device_code").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01 09:00:00").cast("timestamp")) & (F.col("pressure") <= 1014))
    .withColumn("humidity_rounded", F.round("humidity", 0))
    .withColumn("status_bits_rounded", F.round("status_bits", 0))
)

scored = (
    valid
    .filter(F.col("status_bits_rounded").between(24, 67))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4))
    .withColumn("humidity_share_2", F.round((F.col("humidity") / F.sum("humidity").over(w3)), 4))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("prev_status_bits", F.lag("status_bits", 2).over(w_part))
)

metrics = (
    scored
    .select(
        "*",
        F.min("humidity_share_2").over(w3).alias("device_code_min_humidity_share_2"),
        F.lead("temp_c", 2).over(w6).alias("next_temp_c"),
        F.ntile(4).over(w7).alias("humidity_share_bucket"),
        F.percent_rank().over(w8).alias("humidity_percent_rank"),
        F.row_number().over(w9).alias("event_seq_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "humidity",
        "pressure",
        "status_bits_rounded",
        "prev_status_bits",
        "device_code_min_humidity_share_2",
        "next_temp_c",
        "humidity_percent_rank",
        "event_seq_2",
        F.min("humidity_rounded").over(w10).alias("running_min_humidity_rounded"),
        F.count("humidity_rounded").over(w10).alias("running_count_humidity_rounded"),
        F.avg("humidity_rounded").over(w10).alias("running_avg_humidity_rounded"),
        F.max("humidity_rounded").over(w10).alias("running_max_humidity_rounded"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("status_bits_rounded") >= 82, "LARGE").otherwise("SMALL").alias("status_bits_rounded_tier"),
    )
    .agg(
        F.percentile_approx("event_seq_2", [0.25, 0.5, 0.75]).alias("event_seq_2_quartiles"),
        F.percentile_approx("humidity", 0.5).alias("humidity_p50"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("row_count").desc())
)
