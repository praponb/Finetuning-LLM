from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("burst_id").desc(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("device_code") == "DV-03")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("pressure") >= 1029, "High").when(F.col("pressure") >= 988, "Medium").otherwise("Low").alias("pressure_level"),
    )
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "pressure_level",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

gaps_2 = (
    sessionized
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized_device_readings = gaps_2.withColumn("burst_id_2", F.sum("new_group_flag").over(w2))

latest = (
    sessionized_device_readings
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

latest_device_readings = (
    latest
    .withColumn("row_num_2", F.row_number().over(w4))
    .filter(F.col("row_num_2") == 1)
)

flagged = (
    latest_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy("burst_id", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
    )
    .orderBy(F.col("burst_id").desc())
)
