from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)
