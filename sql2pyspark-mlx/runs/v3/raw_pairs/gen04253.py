from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~F.col("page").isin(["/home", "/product", "/search"]) & F.col("duration_sec").between(161, 344) & F.col("event_type").isin(["page_view", "click", "search"]))
    .withColumn("event_type_lower", F.lower("event_type"))
)

page_summary = (
    filtered_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    page_summary
    .filter(F.col("checkout_count") >= 79)
    .orderBy("page")
)
