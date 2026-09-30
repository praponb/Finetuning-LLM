from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("duration_sec") != 463)

summary = (
    filtered_web_events
    .groupBy("page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") > 160)
    .select("page", "avg_duration_sec", "product_count")
    .orderBy("product_count", F.col("avg_duration_sec").desc())
)
