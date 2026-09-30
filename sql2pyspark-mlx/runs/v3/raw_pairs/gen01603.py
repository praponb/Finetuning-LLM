from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("duration_sec").between(268, 465))
    .withColumn("page_filled", F.coalesce("page", F.lit("Unknown")))
)

summary = (
    filtered_web_events
    .groupBy("page")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    summary
    .filter(F.col("duration_sec_range") > 66)
    .select("page", "min_duration_sec", "duration_sec_range")
    .orderBy(F.col("min_duration_sec").desc())
)
