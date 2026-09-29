from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("dim"), F.col("e.session_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.browser").alias("browser"),
        F.col("dim.country").alias("country"),
        F.col("e.page").alias("page"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("cnt").asc_nulls_first())
)
