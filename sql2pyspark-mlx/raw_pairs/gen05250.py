from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 28) | F.col("page").isin(["/home", "/checkout"]))
    .select("id", "session_id")
)
