from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "page"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("src.page").alias("page"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.duration_sec")).alias("max_duration_sec"),
    )
)
