from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-07-06"))) & F.col("page").isNotNull() & (F.col("duration_sec") == 166))
    .withColumn("duration_sec_str", F.col("duration_sec").cast("double"))
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy("event_type", "burst_id")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.sum(F.when(F.col("page") == "/cart", F.col("duration_sec")).otherwise(0)).alias("cart_duration_sec"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("event_type").desc_nulls_first())
)
