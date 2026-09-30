from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "LARGE").otherwise("SMALL").alias("is_new_burst_tier"),
        F.when(F.col("duration_sec") >= 409, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.sum(F.when(F.col("event_type") == "click", F.col("session_no")).otherwise(0)).alias("click_session_no"),
    )
    .orderBy("add_to_cart_events")
)
