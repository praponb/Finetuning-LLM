from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["click", "add_to_cart", "purchase"]))
    .withColumn("clean_page", F.trim("page"))
    .orderBy(F.col("clean_page").desc(), F.col("session_id").desc(), "id")
    .limit(5)
)
