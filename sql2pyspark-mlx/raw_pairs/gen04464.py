from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w_running = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("last_known_duration_sec", "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_web_events
    .filter(F.col("page").eqNullSafe(F.lit("/search")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

base = (
    filtered
    .filter(F.col("page").isNotNull())
    .withColumn("event_type_prefix", F.substring("event_type", 1, 3))
    .withColumn("event_type_page_key", F.concat_ws(",", "event_type", "page"))
)

enriched = (
    base
    .filter(F.col("event_type") != "search")
    .select(
        "id",
        "page",
        "event_type_page_key",
        F.ntile(5).over(w_ordered).alias("duration_sec_bucket"),
        F.last("duration_sec", ignorenulls=True).over(w_running).alias("last_known_duration_sec"),
    )
)

filtered_web_events = (
    enriched
    .filter(F.col("page").isNotNull() & (F.col("last_known_duration_sec") != 202))
    .withColumn("last_known_duration_sec_per_duration_sec_bucket", F.round((F.col("last_known_duration_sec") / F.col("duration_sec_bucket")), 2))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

metrics = (
    filtered_web_events
    .select(
        "*",
        F.max("duration_sec_bucket").over(w3).alias("last7_max_duration_sec_bucket"),
        F.avg("duration_sec_bucket").over(w3).alias("last7_avg_duration_sec_bucket"),
        F.count("duration_sec_bucket").over(w3).alias("last7_count_duration_sec_bucket"),
    )
)

summary = (
    metrics
    .groupBy("page")
    .agg(
        F.percentile_approx("last7_max_duration_sec_bucket", [0.25, 0.5, 0.75]).alias("last7_max_duration_sec_bucket_quartiles"),
        F.round(F.sum("duration_sec_bucket") / F.nullif(F.count(F.when(F.col("page") == "/home", 1)), F.lit(0)), 2).alias("duration_sec_bucket_per_home"),
        F.sum(F.when(F.col("page") == "/product", F.col("last7_avg_duration_sec_bucket")).otherwise(0)).alias("product_last7_avg_duration_sec_bucket"),
        F.round(F.avg("last7_count_duration_sec_bucket"), 2).alias("avg_last7_count_duration_sec_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("distinct_ids") < 1000)
)

result = (
    summary
    .select("page", "product_last7_avg_duration_sec_bucket")
    .orderBy(
        F.col("page").asc_nulls_first(),
        F.col("product_last7_avg_duration_sec_bucket").desc(),
    )
)
