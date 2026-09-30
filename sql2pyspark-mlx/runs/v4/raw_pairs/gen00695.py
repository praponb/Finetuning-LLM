from pyspark.sql import functions as F

result = (
    df_sessions
    .groupBy(F.date_trunc("quarter", "started_at").alias("quarter_start"))
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("desktop_events").desc_nulls_last())
)
