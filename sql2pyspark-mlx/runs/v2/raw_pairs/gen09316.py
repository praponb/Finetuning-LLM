from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & ~F.col("event_type").isin(["page_view", "click", "search"]) & (F.col("event_time") >= "2023-12-04"))
    .withColumn("event_type_upper", F.upper("event_type"))
)
