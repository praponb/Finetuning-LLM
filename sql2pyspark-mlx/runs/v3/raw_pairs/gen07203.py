from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 18, 37, 40]) | (F.col("page") != "/product"))
    .groupBy("page", "event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .filter(F.col("click_count") > 39)
    .orderBy("event_type", F.col("click_count").desc(), "page")
    .limit(50)
)
