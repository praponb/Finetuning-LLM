from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/cart", "/search"]) & F.col("session_id").isin([6, 21]))
    .select("id", "page", "session_id")
)
