from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .withColumn("duration_sec_scaled", F.round((F.col("duration_sec") * 0.9), 2))
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
    .withColumn("page_filled", F.coalesce("page", F.lit("DIRECT")))
)

result = (
    valid
    .groupBy("page")
    .pivot("event_type", ["search", "page_view"])
    .agg(F.avg("duration_sec").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
