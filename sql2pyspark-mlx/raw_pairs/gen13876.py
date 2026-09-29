from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("event_time").asc_nulls_last(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "event_time", "duration_sec")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("duration_sec") >= 406, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.min("event_time").alias("first_seen"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev_2"),
    )
    .orderBy(F.col("duration_sec_tier").asc_nulls_last())
)
