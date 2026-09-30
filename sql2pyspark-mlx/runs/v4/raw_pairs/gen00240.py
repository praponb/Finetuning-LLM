from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_per_cart"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)
