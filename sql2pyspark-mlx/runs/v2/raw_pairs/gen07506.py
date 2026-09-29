from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/home", "/search"]) & (F.col("duration_sec") > 141))
    .groupBy("event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") >= 62)
)
