from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.avg("new_group_flag").alias("avg_new_group_flag"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
    .orderBy(F.col("dv_04_rows").desc_nulls_first())
)
