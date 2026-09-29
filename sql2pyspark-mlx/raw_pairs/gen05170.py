from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("event_time") >= "2025-09-01")
    .withColumn("years_since_event_time", (F.datediff(F.current_date(), "event_time") / 365.25))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("duration_sec") >= 476, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.max("event_time").alias("last_seen"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.countDistinct("id").alias("distinct_ids"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
    )
)
