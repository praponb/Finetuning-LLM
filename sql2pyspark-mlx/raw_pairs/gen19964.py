from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy(F.month("event_time"), "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.orderBy(F.col("duration_sec").desc(), "id")

valid = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/home"))) & (F.col("event_type") == "page_view"))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
)

result = (
    valid
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w))
    .withColumn("duration_sec_avg_30d", F.avg("duration_sec").over(w2))
    .withColumn("duration_sec_count_7d", F.count("duration_sec").over(w3))
    .withColumn("duration_sec_bucket", F.ntile(10).over(w4))
)
