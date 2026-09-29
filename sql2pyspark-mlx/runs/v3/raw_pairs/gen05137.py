from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.count("*").alias("total_count"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/product", 1).otherwise(0))) / F.count("*")), 1).alias("pct_product"),
    )
    .filter(F.col("total_count") > 1)
    .orderBy(F.col("event_year").desc())
    .limit(5)
)
