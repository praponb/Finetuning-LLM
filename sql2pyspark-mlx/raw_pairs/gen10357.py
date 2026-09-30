from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .withColumn("adjusted_duration_sec", F.round((F.col("duration_sec") * 100), 2))
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 392, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["duration_sec_tier", "page"], ["duration_sec_tier"]],
        "duration_sec_tier",
        "page",
    )
    .agg(
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.grouping("duration_sec_tier").alias("gid"),
    )
    .orderBy(F.col("cart_events").desc_nulls_first())
)
