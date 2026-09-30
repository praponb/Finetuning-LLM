from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 15))
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_duration_sec") * 100) / F.sum("total_duration_sec").over(w)), 2))
)
