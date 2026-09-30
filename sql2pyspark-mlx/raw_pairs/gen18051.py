from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy("island_id", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits"))).alias("dv_02_total_status_bits"),
    )
    .orderBy(F.col("dv_02_rows").asc_nulls_first())
)
