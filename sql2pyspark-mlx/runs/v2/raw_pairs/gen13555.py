from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/checkout", "/cart"]))
    .select("event_time", "duration_sec", F.length("page").alias("page_length"))
)
