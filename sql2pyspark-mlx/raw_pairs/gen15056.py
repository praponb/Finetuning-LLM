from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2023-09-12 05:00:00")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessions_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 401, "LARGE").otherwise("SMALL"))
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 6, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .rollup("duration_sec_tier", "burst_id_tier", "week_start")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("num_event_types").desc_nulls_first())
)
