from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 165) & F.col("session_id").isin([10, 17, 34, 36]))
)
