from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/home"]))
    .groupBy("session_id", F.year("event_time").alias("event_year"))
    .agg(F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"))
    .filter(F.col("checkout_count") < 2)
)
