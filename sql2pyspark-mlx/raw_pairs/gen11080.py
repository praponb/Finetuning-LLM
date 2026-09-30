from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_no_tier").orderBy(F.col("span_minutes").desc())

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-03-30 14:00:00").cast("timestamp"))
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.substring("page", 1, 4).alias("page_prefix"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("session_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
        F.when(F.col("duration_sec") >= 502, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
        "page",
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.countDistinct("page").alias("distinct_pages"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
    .filter(F.col("distinct_pages") >= 1)
    .withColumn("span_minutes_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("page").desc())
)
