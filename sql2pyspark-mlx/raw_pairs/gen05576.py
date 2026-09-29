from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-06-07 00:00:00"))) & F.col("page").isNotNull())
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy("page")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.avg(F.when(F.col("event_type") == "purchase", F.col("duration_sec"))).alias("purchase_avg_duration_sec"),
        F.round(F.sum("session_id_2") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("session_id_2_per_add_to_cart"),
    )
)
