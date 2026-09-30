from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_web_events
    .filter(F.col("event_time") < "2025-08-03")
    .groupBy("page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_duration_sec") * 100) / F.sum("total_duration_sec").over(w)), 2))
    .orderBy("page")
)
