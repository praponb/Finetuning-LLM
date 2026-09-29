from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") > 390)
    .drop("min_duration_sec")
    .orderBy(F.col("duration_sec_count").desc())
)
