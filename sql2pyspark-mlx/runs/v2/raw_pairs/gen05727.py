from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 25, 30, 37]) | F.col("page").isin(["/home", "/checkout", "/cart"]))
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.count("*").alias("row_count"),
    )
)
