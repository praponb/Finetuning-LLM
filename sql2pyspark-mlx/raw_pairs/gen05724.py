from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(~((F.col("session_id") == 7) & F.col("session_id").isin([6, 8, 9, 15]) & (F.col("page") == "/search")))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
)

agg = (
    base
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
)

result = agg.filter(F.col("click_count") < 28)
