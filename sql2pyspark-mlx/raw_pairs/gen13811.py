from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("humidity", "id").rowsBetween(-2, -1)
w2 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .withColumn("pressure_avg_2_prior", F.avg("pressure").over(w))
    .withColumn("humidity_avg_15m", F.avg("humidity").over(w2))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

base = (
    deduped
    .filter(F.col("pressure_avg_2_prior") > 1016)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        "pressure_avg_2_prior",
        "humidity_avg_15m",
        "row_num",
        F.when(F.col("temp_c") >= 35.9, "High").when(F.col("temp_c") >= 22, "Medium").otherwise("Low").alias("temp_c_level"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w4).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w4) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w5))

result = (
    sessions_device_readings
    .groupBy(F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.min("reading_ts").alias("started_at"),
        F.sum("humidity_avg_15m").alias("total_humidity_avg_15m"),
        F.min("temp_c").alias("min_temp_c"),
        F.round(F.avg("session_id"), 2).alias("avg_session_id"),
    )
)
