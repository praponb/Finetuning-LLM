from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 303) & (F.col("session_id").isin([10, 13, 17, 26]) | F.col("duration_sec").between(182, 402)))
    .withColumn("event_type_group", F.when(F.col("event_type").isin(["purchase", "add_to_cart"]), "Group A").otherwise("Other"))
    .orderBy("event_time", "duration_sec", "id")
    .limit(10)
)
