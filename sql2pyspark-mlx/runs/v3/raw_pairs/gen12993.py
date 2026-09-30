from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "add_to_cart") & F.col("session_id").isin([14, 22]) & (F.col("session_id") == 38))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
    )
    .orderBy("page")
)
