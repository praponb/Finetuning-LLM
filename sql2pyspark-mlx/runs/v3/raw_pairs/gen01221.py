from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id").isin([26, 29, 31, 33]))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
)

summary = (
    src
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"))
)

result = (
    summary
    .select("event_year", "checkout_count")
    .orderBy(F.col("event_year").desc(), F.col("checkout_count").desc())
)
