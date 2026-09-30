from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.count("*").alias("row_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .orderBy(F.col("row_count").desc())
)
