from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("page_group", F.when(F.col("page").isin(["/cart", "/checkout"]), "Priority").otherwise("Other"))
    .withColumn("event_time_period", F.date_trunc("MONTH", "event_time"))
    .orderBy(F.col("event_time_period").desc())
)
