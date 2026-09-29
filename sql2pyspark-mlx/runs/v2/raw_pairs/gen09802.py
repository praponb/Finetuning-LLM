from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/product")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
