from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.to_date("reading_ts") >= "2023-03-01")
    .select("*", F.when(F.col("firmware") == "2.0.1", 1).otherwise(0).alias("is_2_0_1"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    sessions_device_readings
    .groupBy("island_id")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.min("reading_ts").alias("started_at"),
        F.max("island_id").alias("max_island_id"),
    )
)
