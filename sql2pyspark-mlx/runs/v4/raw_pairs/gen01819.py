from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 12:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("temp_c") >= 38.7, "High").when(F.col("temp_c") >= 19.1, "Medium").otherwise("Low").alias("temp_c_band"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_device_readings
    .groupBy("device_code", F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list_2"),
    )
    .orderBy(F.col("device_code").desc())
)
