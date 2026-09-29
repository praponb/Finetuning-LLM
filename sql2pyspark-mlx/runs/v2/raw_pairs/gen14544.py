from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/home", "/product"]) & (F.col("page") != "/search"))
    .groupBy("event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
    .filter(F.col("click_count") >= 4)
    .orderBy("event_type")
)
