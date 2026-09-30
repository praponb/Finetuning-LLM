from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("event_type") == "add_to_cart") & F.col("page").isNotNull()))
    .withColumn("event_time_date", F.to_date("event_time"))
    .orderBy("event_time_date", "id")
    .limit(10)
)
