from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .withColumn("running_count_duration_sec", F.count("duration_sec").over(w))
    .withColumn("running_avg_duration_sec", F.avg("duration_sec").over(w))
    .withColumn("running_min_duration_sec", F.min("duration_sec").over(w))
    .withColumn("running_sum_duration_sec", F.sum("duration_sec").over(w))
)

result = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "running_count_duration_sec",
        "running_avg_duration_sec",
    )
    .orderBy(F.col("page").desc_nulls_first())
)
