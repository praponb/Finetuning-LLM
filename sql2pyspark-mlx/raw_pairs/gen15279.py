from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_first(), "id")

latest = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .withColumn("activity_month", F.date_trunc("MONTH", "event_time"))
    .rollup("week_start", "activity_month")
    .agg(
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_avg_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.min("event_time").alias("first_seen"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("page_view_avg_duration_sec").desc_nulls_last())
)
