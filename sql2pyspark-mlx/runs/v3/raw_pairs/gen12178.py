from pyspark.sql import functions as F

src = df_web_events.filter(F.col("session_id").isin([6, 14, 32]))

summary = (
    src
    .groupBy("page")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)

result = (
    summary
    .select("page", "duration_sec_range")
    .orderBy("page")
)
