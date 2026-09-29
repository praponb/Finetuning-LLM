from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id")
w2 = Window.partitionBy(F.to_date("event_time"), "page").orderBy(F.col("duration_sec").desc_nulls_first())
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")

result = (
    df_web_events
    .filter(F.col("event_time") < "2023-03-01")
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w2))
    .withColumn("duration_sec_bucket", F.ntile(3).over(w3))
)
