from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_web_events
    .filter(F.col("duration_sec") < 410)
    .groupBy(
        F.when(F.col("duration_sec") >= 365, "high").when(F.col("duration_sec") >= 227, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .withColumn("pct_of_total", F.round(((F.col("total_duration_sec") * 100) / F.sum("total_duration_sec").over(w)), 2))
)
