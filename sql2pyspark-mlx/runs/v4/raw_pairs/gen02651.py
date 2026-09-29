from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-10-29"))) & (F.col("event_type") == "add_to_cart"))
    .withColumn("is_add_to_cart", F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
    )
    .filter(F.col("cart_count") >= 1)
    .orderBy(F.col("duration_sec_bit_or").desc())
)
