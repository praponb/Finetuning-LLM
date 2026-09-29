from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .filter(F.col("min_duration_sec") > 330)
    .orderBy("min_duration_sec", F.col("duration_sec_range").desc())
)
