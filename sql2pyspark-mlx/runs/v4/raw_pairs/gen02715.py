from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart"]) & F.col("page").isin(["/cart", "/product", "/search"]))
    .groupBy("page", "session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
)
