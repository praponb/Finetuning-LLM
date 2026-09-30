from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.round((F.col("status_bits") * 100), 2).alias("adjusted_status_bits"),
        F.substring("firmware", 1, 2).alias("firmware_prefix"),
    )
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("humidity") >= 67, "HIGH").otherwise("LOW").alias("humidity_tier"),
        F.date_trunc("hour", "reading_ts").alias("hour_window"),
        F.when(F.col("pressure") >= 1023, "HIGH").otherwise("LOW").alias("pressure_tier"),
    )
    .agg(
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
    )
    .filter(F.col("dv_03_events") < 1000)
)
