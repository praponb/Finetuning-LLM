from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("device_code")
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("corr_status_bits_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("running_humidity", "id")
w7 = Window.orderBy("running_humidity_share", "id")
w8 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy("running_corr_status_bits_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_device_readings
    .select(
        "*",
        F.count("status_bits").over(w).alias("status_bits_count_2h"),
        F.corr("status_bits", "temp_c").over(w2).alias("corr_status_bits_temp_c"),
        F.sum("humidity").over(w3).alias("running_humidity"),
    )
)

with_windows = (
    metrics
    .withColumn("last_known_humidity", F.last("humidity", ignorenulls=True).over(w3))
    .withColumn("running_corr_status_bits_temp_c", F.sum("corr_status_bits_temp_c").over(w4))
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w5))
    .withColumn("running_humidity_share", F.round((F.col("running_humidity") / F.sum("running_humidity").over(w2)), 4))
)

scored = (
    with_windows
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("status_bits_count_2h_share", F.round((F.col("status_bits_count_2h") / F.sum("status_bits_count_2h").over(w2)), 4))
    .withColumn("row_seq", F.row_number().over(w7))
    .withColumn("last_known_humidity_2", F.last("humidity", ignorenulls=True).over(w8))
    .withColumn("first_known_corr_status_bits_temp_c", F.first("corr_status_bits_temp_c", ignorenulls=True).over(w9))
)

result = (
    scored
    .groupBy(
        F.when(F.col("row_seq") >= 8, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("pressure")).otherwise(0)).alias("dv_02_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("humidity")).otherwise(0)).alias("dv_02_humidity"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.count("*").alias("event_count_2"),
    )
    .orderBy(F.col("activity_day").desc())
)
