from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart", "/search"]) & F.col("event_type").isin(["click", "purchase"]))
    .withColumn("event_time_date", F.to_date("event_time"))
)

web_events_stats = (
    src
    .groupBy(
        F.when(F.col("event_type").isin(["purchase", "add_to_cart"]), "group_a").otherwise("other").alias("event_type_group"),
        "page",
    )
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
)

result = (
    web_events_stats
    .filter(F.col("home_duration_sec") >= 26)
    .select("event_type_group", "page", "latest_event_time")
    .orderBy("event_type_group", F.col("page").desc())
)
