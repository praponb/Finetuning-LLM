from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts_weekofyear").desc())
w2 = Window.partitionBy("device_code").orderBy("humidity", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        (F.col("status_bits") + F.col("pressure")).alias("status_bits_plus_pressure"),
    )
)

cleaned_device_readings = (
    cleaned
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("reading_ts_weekofyear", F.weekofyear("reading_ts"))
)

latest = (
    cleaned_device_readings
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .withColumn("next_status_bits_plus_pressure", F.lead("status_bits_plus_pressure").over(w2))
    .withColumn("prev_status_bits_plus_pressure", F.lag("status_bits_plus_pressure").over(w2))
    .withColumn("next_reading_ts_weekofyear", F.lead("reading_ts_weekofyear").over(w2))
    .withColumn("prev_reading_ts_weekofyear", F.lag("reading_ts_weekofyear").over(w2))
    .withColumn("row_seq", F.row_number().over(w2))
)

result = (
    scored
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("prev_reading_ts_weekofyear"))).alias("dv_04_avg_prev_reading_ts_weekofyear"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
    )
    .orderBy("distinct_firmwares")
)
