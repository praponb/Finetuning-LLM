from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("page").isin(["/checkout", "/search"]), "core").otherwise("other").alias("page_group"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("total_count"),
    )
    .orderBy("page_group")
)
