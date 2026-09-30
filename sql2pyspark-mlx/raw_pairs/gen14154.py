from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(((F.col("event_type") != "click") & F.col("event_type").isin(["search", "click", "add_to_cart"])) | (F.col("event_time") >= "2025-05-31"))
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 491, "High").when(F.col("duration_sec") >= 116, "Medium").otherwise("Low"))
)

page_summary = (
    src
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
)

result = page_summary.select("page", "add_to_cart_duration_sec")
