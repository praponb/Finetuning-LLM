from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"))
    .filter(F.col("home_count") < 64)
    .orderBy(F.col("home_count").desc())
)
