from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page", "session_id").orderBy("duration_sec_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec_adjusted").desc_nulls_first())
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id", "page").orderBy("is_new_session", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy(F.col("cumulative_duration_sec_adjusted").desc(), "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-07-05").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
)

enriched = (
    valid
    .filter(F.col("session_id").isin([10, 15]))
    .withColumn("cumulative_duration_sec_adjusted", F.sum("duration_sec_adjusted").over(w))
    .withColumn("first_known_duration_sec_adjusted", F.first("duration_sec_adjusted", ignorenulls=True).over(w2))
    .withColumn("duration_sec_adjusted_dense_rank", F.dense_rank().over(w3))
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w))

scored = (
    sessionized
    .select(
        "*",
        F.sum("duration_sec_adjusted").over(w5).alias("running_sum_duration_sec_adjusted"),
        F.count("duration_sec_adjusted").over(w5).alias("running_count_duration_sec_adjusted"),
        F.min("duration_sec_adjusted").over(w5).alias("running_min_duration_sec_adjusted"),
        F.ntile(10).over(w6).alias("cumulative_duration_sec_adjusted_bucket"),
    )
)

wide = (
    scored
    .groupBy("session_id")
    .pivot("page", ["/search", "/cart"])
    .agg(F.max("duration_sec_adjusted").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "session_id",
        F.col("/search_total").alias("search_total"),
        F.col("/search_cnt").alias("search_cnt"),
        F.col("/cart_total").alias("cart_total"),
        F.col("/cart_cnt").alias("cart_cnt"),
    )
)

result = (
    wide
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "search_total",
        "search_cnt",
        "cart_total",
        F.col("max_web_event_duration_sec"),
    )
)
