from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.count("*").alias("total_count"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)
