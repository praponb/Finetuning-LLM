from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~(F.col("session_id").isin([3, 18]) & (F.col("page") == "/cart") & F.col("session_id").isin([3, 5, 14])))
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 408, "High").when(F.col("duration_sec") >= 118, "Medium").otherwise("Low"))
)
