from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 23, 24]))
    .withColumn("page_prefix", F.substring("page", 1, 2))
    .withColumn("page_length", F.length("page"))
)
