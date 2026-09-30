from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-12-14") & F.col("page").isin(["/product", "/cart"]) & F.col("session_id").isin([6, 13, 15, 35]))
    .orderBy("event_time")
)
