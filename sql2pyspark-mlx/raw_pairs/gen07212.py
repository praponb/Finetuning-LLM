from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("duration_sec_filled").desc_nulls_last(), "id")
w_hist = Window.partitionBy("event_type").orderBy("session_no", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy(F.col("last3_max_session_no").desc(), "id")

valid = (
    df_web_events
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([17, 20, 36]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

latest = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "event_type", "resolved_page", "new_group_flag", "session_no")
)

enriched = (
    latest
    .filter(F.col("session_no") >= 7)
    .select(
        "*",
        F.count("session_no").over(w_hist).alias("last3_count_session_no"),
        F.max("session_no").over(w_hist).alias("last3_max_session_no"),
        F.avg("session_no").over(w_hist).alias("last3_avg_session_no"),
        F.min("session_no").over(w_hist).alias("last3_min_session_no"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("last3_min_session_no") >= 7.7, "HIGH").otherwise("LOW").alias("last3_min_session_no_tier"),
        "event_type",
        "session_no",
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.sum("last3_max_session_no") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("last3_max_session_no_per_add_to_cart"),
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
