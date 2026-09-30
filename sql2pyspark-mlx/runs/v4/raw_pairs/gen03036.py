from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
    )
    .orderBy("duration_sec_range")
)
