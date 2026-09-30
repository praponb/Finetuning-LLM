from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_web_events
    .withColumn("duration_sec_max_30d", F.max("duration_sec").over(w))
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w2))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .withColumn("duration_sec_count_7d", F.count("duration_sec").over(w4))
    .withColumn("duration_sec_max_30d_max_90d", F.max("duration_sec_max_30d").over(w_hist))
)

with_prev = scored.withColumn("prev_event_time", F.lag("event_time").over(w6))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w7))

result = (
    sessionized
    .groupBy(
        "event_type",
        F.when(F.col("duration_sec_max_30d") >= 437, "HIGH").otherwise("LOW").alias("duration_sec_max_30d_tier"),
        F.when(F.col("duration_sec_count_7d") >= 15, "PREMIUM").otherwise("STANDARD").alias("duration_sec_count_7d_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
        F.count("*").alias("cnt"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("is_new_burst_per_home"),
    )
    .orderBy("duration_sec_max_30d_tier")
)
