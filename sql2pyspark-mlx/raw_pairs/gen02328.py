from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    islands
    .select("id", "reading_ts", "humidity", "status_bits", "firmware", "is_new_burst")
    .orderBy(F.col("reading_ts").asc_nulls_first())
)
