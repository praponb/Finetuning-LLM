from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "purchase", "search"]) & (F.col("page") == "/product") & F.col("session_id").isin([6, 15, 18]))
    .select("session_id", F.ceil("duration_sec").alias("duration_sec_whole"))
)
