from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 5, 39]))
    .withColumn("page_count_duration_sec", F.count("duration_sec").over(w))
    .withColumn("duration_sec_avg_2_prior", F.avg("duration_sec").over(w2))
    .withColumn("duration_sec_avg_5", F.avg("duration_sec").over(w_grp))
)
