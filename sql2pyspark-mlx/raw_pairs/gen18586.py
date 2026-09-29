from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("status_bits") != 9)
    .select(
        "*",
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "firmware",
        "status_bits_plus_humidity",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
    )
)
