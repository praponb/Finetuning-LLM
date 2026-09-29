from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_part = Window.partitionBy("page", "event_type")

with_windows = (
    df_web_events
    .withColumn("duration_sec_avg_2_prior", F.avg("duration_sec").over(w))
    .withColumn("page_stddev_samp_duration_sec", F.stddev_samp("duration_sec").over(w_part))
)

result = (
    with_windows
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_avg_2_prior",
        "page_stddev_samp_duration_sec",
    )
    .orderBy(F.col("event_type").desc())
)
