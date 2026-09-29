from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") >= "2023-03-01") & F.col("duration_sec").between(133, 290) & F.col("duration_sec").between(138, 457))
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .orderBy(F.col("duration_sec_range").desc())
)
