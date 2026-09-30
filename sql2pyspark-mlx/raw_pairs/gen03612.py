from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec_whole")
w2 = Window.partitionBy("session_id").orderBy("duration_sec_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec_whole", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("first_known_duration_sec_whole").asc_nulls_first())
w5 = Window.partitionBy("page")
w6 = Window.orderBy("duration_sec_whole_bucket", "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.trim("page").alias("clean_page"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "event_type",
        "page",
        "clean_page",
        F.first("duration_sec_whole", ignorenulls=True).over(w2).alias("first_known_duration_sec_whole"),
        F.ntile(4).over(w3).alias("duration_sec_whole_bucket"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w4).alias("first_known_duration_sec_whole_dense_rank"),
        F.count("duration_sec_whole_bucket").over(w5).alias("page_count_duration_sec_whole_bucket"),
        F.ntile(3).over(w6).alias("duration_sec_whole_bucket_bucket"),
    )
)

result = (
    enriched
    .groupBy("page")
    .pivot("event_type", ["purchase", "click", "add_to_cart"])
    .agg(
        F.sum("first_known_duration_sec_whole").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
