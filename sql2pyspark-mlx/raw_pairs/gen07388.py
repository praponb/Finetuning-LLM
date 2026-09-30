from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "click"]))
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("cnt"),
    )
)
