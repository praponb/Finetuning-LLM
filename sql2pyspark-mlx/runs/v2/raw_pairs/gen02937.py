from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2025-02-25") & (F.col("session_id") == 31))
    .withColumn("page_group", F.when(F.col("page").isin(["/home", "/search"]), "Group A").otherwise("Other"))
)
