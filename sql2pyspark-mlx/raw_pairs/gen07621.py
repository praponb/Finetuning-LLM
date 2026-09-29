from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("started_at"), "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy(F.to_date("started_at"), "customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device", "browser").orderBy("event_seq", "id")
w6 = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc_nulls_first(), "id")
w7 = Window.orderBy("event_seq", "id")
w8 = Window.partitionBy("customer_id").orderBy("running_avg_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy(F.col("running_count_event_seq").asc_nulls_first())

valid = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-04-05 02:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.initcap(F.trim("device")).alias("device_title"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

scored = (
    valid
    .filter(F.col("customer_id").isin([17, 24]))
    .select(
        "*",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w3).alias("last_known_browser_2"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "device_title",
        "country_clean",
        "event_seq",
        F.min("event_seq").over(w_recent).alias("running_min_event_seq"),
        F.max("event_seq").over(w_recent).alias("running_max_event_seq"),
        F.count("event_seq").over(w_recent).alias("running_count_event_seq"),
        F.avg("event_seq").over(w_recent).alias("running_avg_event_seq"),
        F.ntile(5).over(w_hist).alias("event_seq_bucket"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "device_title",
        "country_clean",
        "running_max_event_seq",
        "running_count_event_seq",
        "running_avg_event_seq",
        "dedup_rank",
        F.row_number().over(w7).alias("seq_no"),
        F.first("running_min_event_seq", ignorenulls=True).over(w8).alias("first_known_running_min_event_seq"),
        F.cume_dist().over(w9).alias("running_count_event_seq_cume_dist"),
    )
)

result = (
    enriched
    .groupBy("customer_id")
    .pivot("country", ["JP", "US", "TH"])
    .agg(F.avg("running_count_event_seq_cume_dist").alias("total"))
    .select(
        "customer_id",
        F.col("JP").alias("jp"),
        F.col("US").alias("us"),
        F.col("TH").alias("th"),
    )
)
