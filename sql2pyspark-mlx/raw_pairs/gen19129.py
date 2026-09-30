from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("event_time")).orderBy(F.col("event_time").desc(), "id")
w_ordered = Window.partitionBy(F.date_trunc("week", "event_time")).orderBy("event_time", "id").rowsBetween(-3, -1)
w_ordered_2 = Window.partitionBy(F.date_trunc("WEEK", "event_time"), "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("session_id", "page")

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-08-01 00:00:00").cast("timestamp")) & (~F.col("page").eqNullSafe(F.lit("/checkout"))))
    .withColumn("event_type_page", F.concat("event_type", F.lit("/"), "page"))
)

result = (
    filtered
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("duration_sec_avg_3_prior", F.avg("duration_sec").over(w_ordered))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w_ordered_2))
    .withColumn("session_id_sum_duration_sec", F.sum("duration_sec").over(w4))
)
