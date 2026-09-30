from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

valid = (
    islands
    .filter((F.col("event_time") > F.lit("2024-03-07 18:00:00").cast("timestamp")) & F.col("session_id").isin([15, 20, 31, 33]))
    .withColumn("duration_sec_times_session_no", (F.col("duration_sec") * F.col("session_no")))
)

result = (
    valid
    .withColumn("duration_sec_times_session_no_tier", F.when(F.col("duration_sec_times_session_no") >= 77, "PREMIUM").otherwise("STANDARD"))
    .withColumn("session_start_flag_tier", F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD"))
    .cube("duration_sec_times_session_no_tier", "session_start_flag_tier")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("session_start_flag")).otherwise(0)).alias("add_to_cart_session_start_flag"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("purchase_rows") >= 1)
    .orderBy(F.col("distinct_event_types").asc_nulls_last())
)
