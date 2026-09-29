from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("status_bits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("WEEK", "reading_ts")).orderBy("status_bits", "id")
w4 = Window.orderBy("status_bits", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_device_readings
    .withColumn("humidity_count_1h", F.count("humidity").over(w))
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w2))
    .withColumn("prev_humidity", F.lag("humidity", 1).over(w3))
    .withColumn("status_bits_avg_6", F.avg("status_bits").over(w4))
)

result = (
    scored
    .groupBy(
        "device_code",
        F.when(F.col("first_known_temp_c") >= 41.2, "HIGH").otherwise("LOW").alias("first_known_temp_c_tier"),
    )
    .agg(
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.min("humidity").alias("min_humidity"),
        F.count("*").alias("row_count"),
        F.percentile_approx("humidity_count_1h", 0.9).alias("humidity_count_1h_p90"),
        F.count_if(F.col("device_code") == "DV-04").alias("dv_04_events"),
        F.min("reading_ts").alias("first_reading_ts"),
    )
    .orderBy(F.col("first_known_temp_c_tier").desc())
)
