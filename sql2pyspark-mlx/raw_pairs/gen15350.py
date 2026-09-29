from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"))
    .filter(F.col("home_count") > 3)
)
