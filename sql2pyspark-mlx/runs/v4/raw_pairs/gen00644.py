from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .rollup("device_code", "session_id_tier", "is_new_session_tier")
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("status_bits")).otherwise(0)).alias("dv_01_status_bits"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.grouping("device_code").alias("grouping_level"),
    )
)
