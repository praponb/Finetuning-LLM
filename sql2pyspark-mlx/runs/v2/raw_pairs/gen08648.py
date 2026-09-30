from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "search"]) & (F.col("page") == "/product"))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["page_view", "purchase"]), "Group A").otherwise("Other"))
)
