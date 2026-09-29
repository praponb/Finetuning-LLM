from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_last(), "id")
w2 = Window.partitionBy("session_id").orderBy("row_num", "id")
w3 = Window.partitionBy("page").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("event_time"), "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy(F.to_date("event_time"), "session_id").orderBy("event_seq", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.date_trunc("week", "event_time")).orderBy("row_num", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("duration_sec") != 349))
    .withColumn("months_since_event_time", (F.datediff(F.current_date(), "event_time") / 30))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "*",
        F.lead("months_since_event_time").over(w2).alias("next_months_since_event_time"),
        F.lead("duration_sec").over(w2).alias("next_duration_sec"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

latest_web_events = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest_web_events
    .withColumn("last_known_months_since_event_time", F.last("months_since_event_time", ignorenulls=True).over(w3))
    .withColumn("months_since_event_time_count_30d", F.count("months_since_event_time").over(w4))
    .withColumn("next_duration_sec_avg_6_prior", F.avg("next_duration_sec").over(w5))
    .withColumn("next_duration_sec_avg_3", F.avg("next_duration_sec").over(w6))
)

result = (
    enriched
    .groupBy("event_type")
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.max("event_time").alias("last_event_time"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("event_type") == "purchase", 1)), F.lit(0)), 2).alias("event_seq_per_purchase"),
    )
    .orderBy(F.col("event_seq_per_purchase").asc_nulls_first())
)
